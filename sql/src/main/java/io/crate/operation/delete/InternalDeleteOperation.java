/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.operation.delete;

import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.analyze.WhereClause;
import io.crate.lucene.LuceneQueryBuilder;
import io.crate.operation.ThreadPools;
import io.crate.operation.collect.EngineSearcher;
import org.apache.lucene.index.*;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.QueryWrapperFilter;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.mapper.internal.VersionFieldMapper;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.DefaultSearchContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchLocalRequest;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

@Singleton
public class InternalDeleteOperation implements DeleteOperation {

    private static final ESLogger LOGGER = Loggers.getLogger(InternalDeleteOperation.class);

    private final ThreadPoolExecutor executor;
    private final int corePoolSize;
    private ClusterService clusterService;
    private IndicesService indicesService;
    private final ScriptService scriptService;
    private final CacheRecycler cacheRecycler;
    private final PageCacheRecycler pageCacheRecycler;
    private LuceneQueryBuilder luceneQueryBuilder;
    private final BigArrays bigArrays;
    private final ThreadPool threadPool;

    @Inject
    public InternalDeleteOperation(ClusterService clusterService,
                                   IndicesService indicesService,
                                   ScriptService scriptService,
                                   CacheRecycler cacheRecycler,
                                   PageCacheRecycler pageCacheRecycler,
                                   LuceneQueryBuilder luceneQueryBuilder,
                                   BigArrays bigArrays,
                                   ThreadPool threadPool) {
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.scriptService = scriptService;
        this.cacheRecycler = cacheRecycler;
        this.pageCacheRecycler = pageCacheRecycler;
        this.luceneQueryBuilder = luceneQueryBuilder;
        this.bigArrays = bigArrays;
        this.threadPool = threadPool;
        executor = (ThreadPoolExecutor) threadPool.executor(ThreadPool.Names.SEARCH);
        corePoolSize = executor.getCorePoolSize();
    }

    @Override
    public ListenableFuture<Long> delete(Map<String, ? extends Collection<Integer>> indexShardMap,
                                         final WhereClause whereClause) throws IOException {

        List<Callable<Long>> callableList = new ArrayList<>();
        for (Map.Entry<String, ? extends Collection<Integer>> entry : indexShardMap.entrySet()) {
            final String index = entry.getKey();
            for (final Integer shardId : entry.getValue()) {
                callableList.add(new Callable<Long>() {
                    @Override
                    public Long call() throws Exception {
                        return delete(index, shardId, whereClause);
                    }
                });
            }
        }
        ListenableFuture<List<Long>> listListenableFuture = ThreadPools.runWithAvailableThreads(
                executor, corePoolSize, callableList, new MergePartialCountFunction());

        return Futures.transform(listListenableFuture, new MergePartialCountFunction());
    }

    @Override
    public long delete(String index, int shardId, WhereClause whereClause) throws IOException {
        IndexService indexService = indicesService.indexServiceSafe(index);
        final IndexShard indexShard = indexService.shardSafe(shardId);

        Engine.Searcher searcher = EngineSearcher.getSearcherWithRetry(indexShard, "delete-operation", null);
        SearchShardTarget shardTarget = new SearchShardTarget(clusterService.localNode().id(), index, shardId);


        /*
        final LuceneDocCollector.CollectorFieldsVisitor collectorFieldsVisitor =
                new LuceneDocCollector.CollectorFieldsVisitor(2);
        collectorFieldsVisitor.addField(UidFieldMapper.NAME);
        collectorFieldsVisitor.addField(VersionFieldMapper.NAME);
        */


        SearchContext searchContext = new DefaultSearchContext(0,
                new ShardSearchLocalRequest(
                        Strings.EMPTY_ARRAY,
                        System.currentTimeMillis(),
                        null
                ),
                shardTarget,
                searcher,
                indexService,
                indexShard,
                scriptService,
                cacheRecycler,
                pageCacheRecycler,
                bigArrays,
                threadPool.estimatedTimeInMillisCounter()
        );
        SearchContext.setCurrent(searchContext);

        try {
            final AtomicLong counter = new AtomicLong(0);
            LuceneQueryBuilder.Context queryCtx =
                    luceneQueryBuilder.convert(whereClause, searchContext, indexService.cache());

            /*
            searcher.searcher().search(queryCtx.query(), new Collector() {

                AtomicReader currentReader;
                NumericDocValues versions;
                final String[] uidValues = new String[1];

                final StoredFieldVisitor uidVisitor = new StoredFieldVisitor() {
                    @Override
                    public Status needsField(FieldInfo fieldInfo) throws IOException {
                        if (uidValues[0] != null) {
                            return Status.STOP;
                        } else if (fieldInfo.name.equals(UidFieldMapper.NAME)) {
                            return Status.YES;
                        } else {
                            return Status.NO;
                        }
                    }

                    @Override
                    public void stringField(FieldInfo fieldInfo, String value) throws IOException {
                        assert fieldInfo.name.equals(UidFieldMapper.NAME);
                        uidValues[0] = value;
                    }
                };

                @Override
                public void setScorer(Scorer scorer) throws IOException {

                }

                @Override
                public void collect(int doc) throws IOException {
                    counter.incrementAndGet();
                    currentReader.document(doc, uidVisitor);

                    if (uidValues[0] == null) {
                        return;
                    }
                    Uid uid  = Uid.createUid(uidValues[0]);
                    try {
                        Engine.Delete delete = indexShard.prepareDelete(Constants.DEFAULT_MAPPING_TYPE,
                                uid.id(), versions.get(doc), VersionType.INTERNAL, Engine.Operation.Origin.PRIMARY);
                        indexShard.delete(delete);
                        uidValues[0] = null;
                    } catch (VersionConflictEngineException e) {
                        LOGGER.warn(e.getMessage(), e);
                    }
                }

                @Override
                public void setNextReader(AtomicReaderContext context) throws IOException {
                    currentReader = context.reader();
                    versions = currentReader.getNumericDocValues(VersionFieldMapper.NAME);
                }

                @Override
                public boolean acceptsDocsOutOfOrder() {
                    return true;
                }
            });

            return counter.get();
            */

            QueryWrapperFilter filter = new QueryWrapperFilter(queryCtx.query());
            return innerDelete(indexShard, searcher, filter);
        } finally {
            searchContext.close();
            SearchContext.removeCurrent();
        }
    }

    private long innerDelete(IndexShard indexShard,
                             Engine.Searcher searcher,
                             QueryWrapperFilter filter) throws IOException {
        final String[] uidValues = new String[1];
        final StoredFieldVisitor uidVisitor = new StoredFieldVisitor() {
            @Override
            public Status needsField(FieldInfo fieldInfo) throws IOException {
                if (uidValues[0] != null) {
                    return Status.STOP;
                } else if (fieldInfo.name.equals(UidFieldMapper.NAME)) {
                    return Status.YES;
                } else {
                    return Status.NO;
                }
            }

            @Override
            public void stringField(FieldInfo fieldInfo, String value) throws IOException {
                assert fieldInfo.name.equals(UidFieldMapper.NAME);
                uidValues[0] = value;
            }
        };


        IndexWriter writer = indexShard.writer();

        long deleted = 0;
        for (AtomicReaderContext context : searcher.reader().leaves()) {
            AtomicReader leafReader = context.reader();
            DocIdSet docIdSet = filter.getDocIdSet(context, leafReader.getLiveDocs());
            if (docIdSet == null) {
                continue;
            }
            DocIdSetIterator docIdSetIterator = docIdSet.iterator();
            if (docIdSetIterator == null) {
                continue;
            }
            NumericDocValues versions = leafReader.getNumericDocValues(VersionFieldMapper.NAME);

            while (true) {
                int doc = docIdSetIterator.nextDoc();
                if (doc == DocIdSetIterator.NO_MORE_DOCS) {
                    break;
                }
                leafReader.document(doc, uidVisitor);
                if (uidValues[0] == null) {
                    continue;
                }
                try {
                    boolean b = writer.tryDeleteDocument(leafReader, doc);
                    if (b) {
                        deleted++;
                    }
                } catch (VersionConflictEngineException e) {
                    LOGGER.warn(e.getMessage(), e);
                } finally {
                    uidValues[0] = null;
                }
            }
        }
        return deleted;
    }

    private static class MergePartialCountFunction implements Function<List<Long>, Long> {
        @Nullable
        @Override
        public Long apply(List<Long> partialResults) {
            long result = 0L;
            for (Long partialResult : partialResults) {
                result += partialResult;
            }
            return result;
        }
    }
}
