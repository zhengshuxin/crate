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

import io.crate.analyze.WhereClause;
import io.crate.lucene.LuceneQueryBuilder;
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
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.Uid;
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

import java.io.IOException;

@Singleton
public class InternalDeleteOperation implements DeleteOperation {

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
    }

    @Override
    public long delete(String index, int shardId, WhereClause whereClause) throws IOException {
        IndexService indexService = indicesService.indexServiceSafe(index);
        IndexShard indexShard = indexService.shardSafe(shardId);

        Engine.Searcher searcher = EngineSearcher.getSearcherWithRetry(indexShard, "delete-operation", null);
        SearchShardTarget shardTarget = new SearchShardTarget(clusterService.localNode().id(), index, shardId);

        final String[] uidValues = new String[1];
        StoredFieldVisitor uidVisitor = new StoredFieldVisitor() {
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
            LuceneQueryBuilder.Context queryCtx =
                    luceneQueryBuilder.convert(whereClause, searchContext, indexService.cache());
            QueryWrapperFilter filter = new QueryWrapperFilter(queryCtx.query());
            return innerDelete(indexShard, searcher, filter, uidValues, uidVisitor);
        } finally {
            searchContext.close();
            SearchContext.removeCurrent();
        }
    }

    private long innerDelete(IndexShard indexShard,
                             Engine.Searcher searcher,
                             QueryWrapperFilter filter,
                             String[] uidValues,
                             StoredFieldVisitor uidVisitor) throws IOException {
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

                Uid uid  = Uid.createUid(uidValues[0]);
                indexShard.delete(new Engine.Delete(uid.type(),
                        uid.id(),
                        new Term(UidFieldMapper.NAME, uidValues[0]),
                        versions.get(doc),
                        VersionType.INTERNAL,
                        Engine.Operation.Origin.PRIMARY,
                        System.nanoTime(),
                        false)
                );
                deleted++;
            }
        }
        return deleted;
    }
}
