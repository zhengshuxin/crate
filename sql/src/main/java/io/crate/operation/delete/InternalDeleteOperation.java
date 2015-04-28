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
import com.google.common.collect.Lists;
import io.crate.breaker.RamAccountingContext;
import io.crate.exceptions.TableUnknownException;
import io.crate.exceptions.UnhandledServerException;
import io.crate.jobs.JobContextService;
import io.crate.jobs.JobExecutionContext;
import io.crate.operation.ThreadPools;
import io.crate.operation.collect.CrateCollector;
import io.crate.operation.collect.JobCollectContext;
import io.crate.operation.collect.ShardCollectService;
import io.crate.planner.node.dml.DeleteByQueryNode;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.IndexShardMissingException;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


@Singleton
public class InternalDeleteOperation implements DeleteOperation {

    private static final ESLogger LOGGER = Loggers.getLogger(InternalDeleteOperation.class);
    public static final String EXECUTOR_NAME = ThreadPool.Names.SEARCH;

    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final JobContextService jobContextService;
    private final ThreadPool threadPool;

    @Inject
    public InternalDeleteOperation(ClusterService clusterService,
                                   IndicesService indicesService,
                                   JobContextService collectContextService,
                                   ThreadPool threadPool) {
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.jobContextService = collectContextService;
        this.threadPool = threadPool;
    }

    @Override
    public void delete(DeleteByQueryNode deleteNode,
                       RamAccountingContext ramAccountingContext,
                       final ActionListener<Long> listener) {
        String localNodeId = clusterService.state().nodes().localNodeId();
        int jobSearchContextId = deleteNode.routing().jobSearchContextIdBase();

        assert deleteNode.jobId().isPresent() : "jobId must be set on deleteNode";
        JobExecutionContext jobExecutionContext = jobContextService.getContext(deleteNode.jobId().get());
        JobCollectContext jobCollectContext = jobExecutionContext.getCollectContext(deleteNode.executionNodeId());
        final int numShards = deleteNode.routing().numShards(clusterService.state().nodes().localNodeId());
        List<CrateCollector> shardCollectors = new ArrayList<>(numShards);

        Map<String, Map<String, List<Integer>>> locations = deleteNode.routing().locations();

        assert locations != null : "routing locations is null";

        ActionListener<Long> shardListener = new DeleteShardListener(numShards, listener);

        for (Map.Entry<String, Map<String, List<Integer>>> nodeEntry : locations.entrySet()) {
            if (nodeEntry.getKey().equals(localNodeId)) {
                Map<String, List<Integer>> shardIdMap = nodeEntry.getValue();
                for (Map.Entry<String, List<Integer>> entry : shardIdMap.entrySet()) {
                    String indexName = entry.getKey();
                    IndexService indexService;
                    try {
                        indexService = indicesService.indexServiceSafe(indexName);
                    } catch (IndexMissingException e) {
                        throw new TableUnknownException(entry.getKey(), e);
                    }

                    for (Integer shardId : entry.getValue()) {
                        jobCollectContext.registerJobContextId(indexService.shardSafe(shardId).shardId(), jobSearchContextId);
                        Injector shardInjector;
                        try {
                            shardInjector = indexService.shardInjectorSafe(shardId);
                            ShardCollectService shardCollectService = shardInjector.getInstance(ShardCollectService.class);
                            CrateCollector collector = shardCollectService.getDeleteCollector(
                                    deleteNode,
                                    jobCollectContext,
                                    jobSearchContextId,
                                    shardListener
                            );
                            shardCollectors.add(collector);
                        } catch (IndexShardMissingException e) {
                            throw new UnhandledServerException(
                                    String.format(Locale.ENGLISH, "unknown shard id %d on index '%s'",
                                            shardId, entry.getKey()), e);
                        } catch (Exception e) {
                            LOGGER.error("Error while getting collector", e);
                            throw new UnhandledServerException(e);
                        }
                        jobSearchContextId++;

                    }
                }
            } else {
                // just increase jobSearchContextId by shard size of foreign node(s) indices
                for (List<Integer> shardIdMap : nodeEntry.getValue().values()) {
                    jobSearchContextId += shardIdMap.size();
                }
            }
        }
        runDeleteThreaded(shardCollectors, ramAccountingContext);
    }

    private void runDeleteThreaded(List<CrateCollector> shardCollectors,
                                   final RamAccountingContext ramAccountingContext) {
        ThreadPoolExecutor executor = (ThreadPoolExecutor)threadPool.executor(EXECUTOR_NAME);
        ThreadPools.runWithAvailableThreads(
                executor,
                executor.getCorePoolSize(),
                Lists.transform(shardCollectors, new Function<CrateCollector, Runnable>() {
                    @Nullable
                    @Override
                    public Runnable apply(final CrateCollector input) {
                        return new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    input.doCollect(ramAccountingContext);
                                } catch (Exception e) {
                                    LOGGER.error("error collecting rows for delete", e);
                                }
                            }
                        };
                    }
                })
        );
    }

    private static class DeleteShardListener implements ActionListener<Long> {
        private final ActionListener<Long> listener;
        AtomicInteger pending;
        AtomicLong aggregated;
        AtomicBoolean alreadyFailed;

        public DeleteShardListener(int numShards, ActionListener<Long> finalListener) {
            this.listener = finalListener;
            this.pending = new AtomicInteger(numShards);
            this.aggregated = new AtomicLong(0L);
            this.alreadyFailed = new AtomicBoolean(false);
        }

        @Override
        public void onResponse(Long aLong) {
            if (aLong != null) {
                aggregated.addAndGet(aLong);
            }
            if (pending.decrementAndGet() == 0) {
                LOGGER.trace("all delete operations finished");
                listener.onResponse(aggregated.get());
            }
        }

        @Override
        public void onFailure(Throwable e) {
            LOGGER.trace("delete failed", e);
            if (alreadyFailed.compareAndSet(false, true)) {
                listener.onFailure(e);
            }
        }
    }
}