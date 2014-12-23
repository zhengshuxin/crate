/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.executor.transport.task.elasticsearch;

import com.carrotsearch.hppc.IntArrayList;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.action.sql.query.QueryShardRequest;
import io.crate.action.sql.query.TransportQueryShardAction;
import io.crate.exceptions.Exceptions;
import io.crate.exceptions.FailedShardsException;
import io.crate.executor.*;
import io.crate.metadata.*;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.planner.node.dql.QueryThenFetchNode;
import io.crate.planner.symbol.*;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.action.SearchServiceListener;
import org.elasticsearch.search.action.SearchServiceTransportAction;
import org.elasticsearch.search.controller.SearchPhaseController;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.QueryFetchSearchResult;
import org.elasticsearch.search.fetch.ShardFetchSearchRequest;
import org.elasticsearch.search.internal.InternalScrollSearchRequest;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.query.QueryPhaseExecutionException;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class QueryThenFetchTask extends JobTask implements PagableTask {

    private final ESLogger logger = Loggers.getLogger(this.getClass());

    private Optional<PageInfo> pageInfo;
    private Optional<TimeValue> keepAlive;
    private volatile int limit;
    private volatile int offset;

    private final QueryThenFetchNode searchNode;
    private final TransportQueryShardAction transportQueryShardAction;
    private final SearchServiceTransportAction searchServiceTransportAction;
    private final SearchPhaseController searchPhaseController;
    private final ThreadPool threadPool;
    private final SettableFuture<TaskResult> result;
    private final List<ListenableFuture<TaskResult>> results;

    private final Routing routing;
    private final AtomicArray<IntArrayList> docIdsToLoad;
    private final AtomicArray<QuerySearchResult> firstResults;
    private final AtomicArray<FetchSearchResult> fetchResults;
    private final DiscoveryNodes nodes;
    private final int numColumns;
    private final int numShards;
    private final ClusterState state;
    private final List<FieldExtractor<SearchHit>> extractors;
    volatile ScoreDoc[] sortedShardList;
    private volatile AtomicArray<ShardSearchFailure> shardFailures;
    private final Object shardFailuresMutex = new Object();
    private final Map<SearchShardTarget, Long> searchContextIds;
    private List<Tuple<String, QueryShardRequest>> requests;
    private List<Reference> references;

    /**
     * dummy request required to re-use the searchService transport
     */
    private final static SearchRequest EMPTY_SEARCH_REQUEST = new SearchRequest();


    public QueryThenFetchTask(UUID jobId,
                              Functions functions,
                              QueryThenFetchNode searchNode,
                              ClusterService clusterService,
                              TransportQueryShardAction transportQueryShardAction,
                              SearchServiceTransportAction searchServiceTransportAction,
                              SearchPhaseController searchPhaseController,
                              ThreadPool threadPool) {
        super(jobId);
        this.searchNode = searchNode;
        this.transportQueryShardAction = transportQueryShardAction;
        this.searchServiceTransportAction = searchServiceTransportAction;
        this.searchPhaseController = searchPhaseController;
        this.threadPool = threadPool;

        state = clusterService.state();
        nodes = state.nodes();

        result = SettableFuture.create();
        results = Arrays.<ListenableFuture<TaskResult>>asList(result);

        routing = searchNode.routing();
        pageInfo = Optional.absent();
        this.limit = searchNode.limit();
        this.offset = searchNode.offset();

        Context context = new Context(functions);
        Visitor fieldExtractorVisitor = new Visitor(searchNode.partitionBy());
        extractors = new ArrayList<>(searchNode.outputs().size());
        for (Symbol symbol : searchNode.outputs()) {
            extractors.add(fieldExtractorVisitor.process(symbol, context));
        }
        references = context.references;
        numShards = searchNode.routing().numShards();

        searchContextIds = new ConcurrentHashMap<>(numShards);
        docIdsToLoad = new AtomicArray<>(numShards);
        firstResults = new AtomicArray<>(numShards);
        fetchResults = new AtomicArray<>(numShards);
        numColumns = searchNode.outputs().size();
        keepAlive = Optional.absent();
    }

    private static FieldExtractor<SearchHit> buildExtractor(Reference reference, List<ReferenceInfo> partitionBy) {
        final ColumnIdent columnIdent = reference.info().ident().columnIdent();
        if (DocSysColumns.VERSION.equals(columnIdent)) {
            return new ESFieldExtractor() {
                @Override
                public Object extract(SearchHit hit) {
                    return hit.getVersion();
                }
            };
        } else if (DocSysColumns.ID.equals(columnIdent)) {
            return new ESFieldExtractor() {
                @Override
                public Object extract(SearchHit hit) {
                    return new BytesRef(hit.getId());
                }
            };
        } else if (DocSysColumns.DOC.equals(columnIdent)) {
            return new ESFieldExtractor() {
                @Override
                public Object extract(SearchHit hit) {
                    return hit.getSource();
                }
            };
        } else if (DocSysColumns.RAW.equals(columnIdent)) {
            return new ESFieldExtractor() {
                @Override
                public Object extract(SearchHit hit) {
                    return hit.getSourceRef().toBytesRef();
                }
            };
        } else if (DocSysColumns.SCORE.equals(columnIdent)) {
            return new ESFieldExtractor() {
                @Override
                public Object extract(SearchHit hit) {
                    return hit.getScore();
                }
            };
        } else if (partitionBy.contains(reference.info())) {
            return new ESFieldExtractor.PartitionedByColumnExtractor(reference, partitionBy);
        } else {
            return new ESFieldExtractor.Source(columnIdent);
        }
    }

    @Override
    public void start() {

        // create initial requests
        requests = prepareRequests(references);

        if (!routing.hasLocations() || requests.size() == 0) {
            result.set(PagableTaskResult.EMPTY_PAGABLE_RESULT);
        }

        state.blocks().globalBlockedRaiseException(ClusterBlockLevel.READ);
        AtomicInteger totalOps = new AtomicInteger(0);

        int requestIdx = -1;
        for (Tuple<String, QueryShardRequest> requestTuple : requests) {
            requestIdx++;
            state.blocks().indexBlockedRaiseException(ClusterBlockLevel.READ, requestTuple.v2().index());
            transportQueryShardAction.execute(
                    requestTuple.v1(),
                    requestTuple.v2(),
                    new QueryShardResponseListener(requestIdx, firstResults, totalOps)
            );
        }
    }

    private List<Tuple<String, QueryShardRequest>> prepareRequests(List<Reference> outputs) {
        List<Tuple<String, QueryShardRequest>> requests = new ArrayList<>();
        Map<String, Map<String, Set<Integer>>> locations = searchNode.routing().locations();
        if (locations == null) {
            return requests;
        }
        for (Map.Entry<String, Map<String, Set<Integer>>> entry : locations.entrySet()) {
            String node = entry.getKey();
            for (Map.Entry<String, Set<Integer>> indexEntry : entry.getValue().entrySet()) {
                String index = indexEntry.getKey();
                Set<Integer> shards = indexEntry.getValue();

                for (Integer shard : shards) {
                    requests.add(new Tuple<>(
                            node,
                            new QueryShardRequest(
                                    index,
                                    shard,
                                    outputs,
                                    searchNode.orderBy(),
                                    searchNode.reverseFlags(),
                                    searchNode.nullsFirst(),
                                    limit,
                                    offset,
                                    searchNode.whereClause(),
                                    searchNode.partitionBy(),
                                    pageInfo,
                                    keepAlive
                            )
                    ));
                }
            }
        }
        return requests;
    }

    private void moveToSecondPhase() throws IOException {

        ScoreDoc[] lastEmittedDocs = null;
        if (pageInfo.isPresent()) {
            lastEmittedDocs = searchPhaseController.getLastEmittedDocPerShard(
                    searchPhaseController.sortDocs(true, firstResults),
                    numShards);
        }

        // no scroll used yet, we want the offset to be applied on the first request
        sortedShardList = searchPhaseController.sortDocs(false, firstResults);

        searchPhaseController.fillDocIdsToLoad(docIdsToLoad, sortedShardList);

        if (docIdsToLoad.asList().isEmpty()) {
            finish();
            return;
        }

        final AtomicInteger counter = new AtomicInteger(docIdsToLoad.asList().size());

        for (AtomicArray.Entry<IntArrayList> entry : docIdsToLoad.asList()) {
            QuerySearchResult queryResult = firstResults.get(entry.index);
            DiscoveryNode node = nodes.get(queryResult.shardTarget().nodeId());
            ShardFetchSearchRequest fetchRequest = createFetchRequest(queryResult, entry, lastEmittedDocs);
            executeFetch(entry.index, queryResult.shardTarget(), counter, fetchRequest, node);
        }
    }

    private void executeFetch(final int shardIndex,
                              final SearchShardTarget shardTarget,
                              final AtomicInteger counter,
                              ShardFetchSearchRequest shardFetchSearchRequest,
                              DiscoveryNode node) {

        searchServiceTransportAction.sendExecuteFetch(
                node,
                shardFetchSearchRequest,
                new SearchServiceListener<FetchSearchResult>() {
                    @Override
                    public void onResult(FetchSearchResult result) {
                        result.shardTarget(shardTarget);
                        fetchResults.set(shardIndex, result);
                        if (counter.decrementAndGet() == 0) {
                            finish();
                        }
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        docIdsToLoad.set(shardIndex, null);
                        addShardFailure(shardIndex, shardTarget, t);
                        if (counter.decrementAndGet() == 0) {
                            finish();
                        }
                    }
                }
        );
    }

    private void addShardFailure(int shardIndex, SearchShardTarget shardTarget, Throwable t) {
        if (TransportActions.isShardNotAvailableException(t)) {
            return;
        }
        if (shardFailures == null) {
            synchronized (shardFailuresMutex) {
                if (shardFailures == null) {
                    shardFailures = new AtomicArray<>(requests.size());
                }
            }
        }
        ShardSearchFailure failure = shardFailures.get(shardIndex);
        if (failure == null) {
            shardFailures.set(shardIndex, new ShardSearchFailure(t, shardTarget));
        } else {
            // the failure is already present, try and not override it with an exception that is less meaningless
            // for example, getting illegal shard state
            if (TransportActions.isReadOverrideException(t)) {
                shardFailures.set(shardIndex, new ShardSearchFailure(t, shardTarget));
            }
        }
    }

    private void finish() {
        try {
            threadPool.executor(ThreadPool.Names.SEARCH).execute(new Runnable() {
                @Override
                public void run() {

                    try {
                        if(shardFailures != null && shardFailures.length() > 0){
                            FailedShardsException ex = new FailedShardsException(shardFailures.toArray(
                                    new ShardSearchFailure[shardFailures.length()]));
                            result.setException(ex);
                            return;
                        }
                        InternalSearchResponse response = searchPhaseController.merge(sortedShardList, firstResults, fetchResults);
                        final SearchHit[] hits = response.hits().hits();
                        final Object[][] rows = new Object[hits.length][numColumns];

                        for (int r = 0; r < hits.length; r++) {
                            rows[r] = new Object[numColumns];
                            for (int c = 0; c < numColumns; c++) {
                                rows[r][c] = extractors.get(c).extract(hits[r]);
                            }
                        }

                        if (pageInfo.isPresent()) {
                            result.set(new QTFScrollTaskResult(rows, true));
                        } else {
                            result.set(SinglePageTaskResult.singlePage(rows));
                        }
                    } catch (Throwable t) {
                        result.setException(t);
                    } finally {
                        releaseIrrelevantSearchContexts(firstResults, docIdsToLoad);
                    }
                }
            });
        } catch (EsRejectedExecutionException e) {
            try {
                releaseIrrelevantSearchContexts(firstResults, docIdsToLoad);
            } finally {
                result.setException(e);
            }
        }
    }

    class QTFScrollTaskResult implements PagableTaskResult {

        private final Object[][] rows;
        private final boolean hasNextPage;
        private final SettableFuture<PagableTaskResult> future;

        public QTFScrollTaskResult(Object[][] rows, boolean hasNextPage) {
            this.rows = rows;
            this.hasNextPage = hasNextPage;
            future = SettableFuture.create();
        }

        @Override
        public ListenableFuture<PagableTaskResult> fetch(PageInfo pageInfo) {
            if (!hasNextPage) {
                throw new NoSuchElementException();
            } else {
                SearchScrollRequest dummyScrollRequest = new SearchScrollRequest();
                if (keepAlive.isPresent()) {
                    dummyScrollRequest.scroll(new Scroll(keepAlive.get()));
                }

                final AtomicInteger numOps = new AtomicInteger(numShards);
                final AtomicArray<QueryFetchSearchResult> queryFetchResults = new AtomicArray<>(numShards);

                for (final Map.Entry<SearchShardTarget, Long> entry : searchContextIds.entrySet()) {
                    DiscoveryNode node = nodes.get(entry.getKey().nodeId());

                    InternalScrollSearchRequest internalRequest = new InternalScrollSearchRequest(dummyScrollRequest, entry.getValue());
                    searchServiceTransportAction.sendExecuteFetch(node, internalRequest, new SearchServiceListener<QueryFetchSearchResult>() {
                        @Override
                        public void onResult(QueryFetchSearchResult result) {

                            result.shardTarget(entry.getKey());
                            queryFetchResults.set(entry.getKey().getShardId(), result);

                            if (numOps.decrementAndGet() == 0) {
                                try {
                                    // TODO: execute in thread pool
                                    ScoreDoc[] sortedShardList = searchPhaseController.sortDocs(true, queryFetchResults);
                                    InternalSearchResponse response = searchPhaseController.merge(sortedShardList, queryFetchResults, queryFetchResults);
                                    final SearchHit[] hits = response.hits().hits();
                                    final Object[][] rows = new Object[hits.length][numColumns];

                                    for (int r = 0; r < hits.length; r++) {
                                        rows[r] = new Object[numColumns];
                                        for (int c = 0; c < numColumns; c++) {
                                            rows[r][c] = extractors.get(c).extract(hits[r]);
                                        }
                                    }
                                    future.set(new QTFScrollTaskResult(rows, rows.length > 0));
                                } catch (Throwable e) {
                                    // TODO
                                    e.printStackTrace();
                                }
                            }
                        }

                        @Override
                        public void onFailure(Throwable t) {
                            future.setException(t);
                        }
                    });

                }
            }
            return future;
        }

        @Override
        public Object[][] rows() {
            return rows;
        }

        @javax.annotation.Nullable
        @Override
        public String errorMessage() {
            return null;
        }
    }

    private void releaseIrrelevantSearchContexts(AtomicArray<QuerySearchResult> firstResults,
                                                 AtomicArray<IntArrayList> docIdsToLoad) {

        // don't release searchcontexts yet, if we use scroll
        if (docIdsToLoad == null || pageInfo.isPresent()) {
            return;
        }

        for (AtomicArray.Entry<QuerySearchResult> entry : firstResults.asList()) {
            if (docIdsToLoad.get(entry.index) == null) {
                DiscoveryNode node = nodes.get(entry.value.queryResult().shardTarget().nodeId());
                if (node != null) {
                    searchServiceTransportAction.sendFreeContext(node, entry.value.queryResult().id(), EMPTY_SEARCH_REQUEST);
                }
            }
        }
    }

    protected ShardFetchSearchRequest createFetchRequest(QuerySearchResult queryResult,
                                                    AtomicArray.Entry<IntArrayList> entry, @Nullable ScoreDoc[] lastEmittedDocsPerShard) {
        if (lastEmittedDocsPerShard != null) {
            ScoreDoc lastEmittedDoc = lastEmittedDocsPerShard[entry.index];
            return new ShardFetchSearchRequest(EMPTY_SEARCH_REQUEST, queryResult.id(), entry.value, lastEmittedDoc);
        }
        return new ShardFetchSearchRequest(EMPTY_SEARCH_REQUEST, queryResult.id(), entry.value);
    }

    @Override
    public List<ListenableFuture<TaskResult>> result() {
        return results;
    }

    @Override
    public void upstreamResult(List<ListenableFuture<TaskResult>> result) {
        throw new UnsupportedOperationException("Can't have upstreamResults");
    }

    @Override
    public void setPaging(PageInfo pageInfo) {
        this.pageInfo = Optional.of(pageInfo);

        // override initial limit to size of first page
        this.limit = pageInfo.size();
    }

    /**
     * set the keep alive value for the search context on the shards
     */
    public void setKeepAlive(TimeValue keepAlive) {
        this.keepAlive = Optional.of(keepAlive);
    }

    class QueryShardResponseListener implements ActionListener<QuerySearchResult> {

        private final int requestIdx;
        private final AtomicArray<QuerySearchResult> firstResults;
        private final AtomicInteger totalOps;
        private final int expectedOps;

        public QueryShardResponseListener(int requestIdx,
                                          AtomicArray<QuerySearchResult> firstResults,
                                          AtomicInteger totalOps) {

            this.requestIdx = requestIdx;
            this.firstResults = firstResults;
            this.totalOps = totalOps;
            this.expectedOps = firstResults.length();
        }

        @Override
        public void onResponse(QuerySearchResult querySearchResult) {
            Tuple<String, QueryShardRequest> requestTuple = requests.get(requestIdx);
            QueryShardRequest request = requestTuple.v2();


            querySearchResult.shardTarget(
                    new SearchShardTarget(requestTuple.v1(), request.index(), request.shardId()));
            searchContextIds.put(querySearchResult.shardTarget(), querySearchResult.id());
            firstResults.set(requestIdx, querySearchResult);
            if (totalOps.incrementAndGet() == expectedOps) {
                try {
                    moveToSecondPhase();
                } catch (IOException e) {
                    raiseEarlyFailure(e);
                }
            }
        }

        @Override
        public void onFailure(Throwable e) {
            raiseEarlyFailure(e);
        }
    }

    private void raiseEarlyFailure(Throwable t) {
        for (AtomicArray.Entry<QuerySearchResult> entry : firstResults.asList()) {
            try {
                DiscoveryNode node = nodes.get(entry.value.shardTarget().nodeId());
                if (node != null) {
                    searchServiceTransportAction.sendFreeContext(node, entry.value.id(), EMPTY_SEARCH_REQUEST);
                }
            } catch (Throwable t1) {
                logger.trace("failed to release context", t1);
            }
        }
        t = Exceptions.unwrap(t);
        if (t instanceof QueryPhaseExecutionException) {
            result.setException(t.getCause());
            return;
        }
        result.setException(t);
    }

    static class Context {
        private Functions functions;
        List<Reference> references = new ArrayList<>();

        public Context(Functions functions) {
            this.functions = functions;
        }

        public void addReference(Reference reference) {
            references.add(reference);
        }
    }
    static class Visitor extends SymbolVisitor<Context, FieldExtractor<SearchHit>> {

        private List<ReferenceInfo> partitionBy;

        public Visitor(List<ReferenceInfo> partitionBy) {
            this.partitionBy = partitionBy;
        }

        @Override
        protected FieldExtractor<SearchHit> visitSymbol(Symbol symbol, Context context) {
            throw new UnsupportedOperationException(
                    SymbolFormatter.format("QueryThenFetch doesn't support \"%s\" in outputs", symbol));
        }

        @Override
        public FieldExtractor<SearchHit> visitReference(final Reference reference, Context context) {
            context.addReference(reference);
            return buildExtractor(reference, partitionBy);
        }

        @Override
        public FieldExtractor<SearchHit> visitDynamicReference(DynamicReference symbol, Context context) {
            return visitReference(symbol, context);
        }

        @Override
        public FieldExtractor<SearchHit> visitFunction(Function symbol, Context context) {
            List<FieldExtractor<SearchHit>> subExtractors = new ArrayList<>(symbol.arguments().size());
            for (Symbol argument : symbol.arguments()) {
                subExtractors.add(process(argument, context));
            }
            Scalar scalar = (Scalar) context.functions.getSafe(symbol.info().ident());
            return new FunctionExtractor<>(scalar, subExtractors);
        }

        @Override
        public FieldExtractor<SearchHit> visitLiteral(Literal symbol, Context context) {
            return new LiteralExtractor<>(symbol.value());
        }
    }
}
