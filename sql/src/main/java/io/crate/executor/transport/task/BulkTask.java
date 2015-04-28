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

package io.crate.executor.transport.task;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.executor.JobTask;
import io.crate.executor.RowCountResult;
import io.crate.executor.Task;
import io.crate.executor.TaskResult;
import io.crate.executor.transport.TransportExecutor;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class BulkTask extends JobTask {

    private final TransportExecutor transportExecutor;
    private final List<Task> subTasks;
    private final List<ListenableFuture<TaskResult>> resultList;
    private final SettableFuture<TaskResult> futureResult;

    public BulkTask(TransportExecutor transportExecutor, UUID jobId, List<Task> subTasks) {
        super(jobId);
        this.transportExecutor = transportExecutor;
        this.subTasks = subTasks;

        if (subTasks.size() == 0) {
            resultList = new ArrayList<>(1);
            resultList.add(Futures.immediateFuture((TaskResult) RowCountResult.EMPTY_RESULT));
            futureResult = null;
        } else if (subTasks.size() == 1) {
            // just forward results from sub task
            List<ListenableFuture<TaskResult>> subTaskResult = subTasks.get(0).result();
            if (subTaskResult instanceof ImmutableList) {
                resultList = new ArrayList<>();
            } else {
                resultList = subTaskResult;
            }
            futureResult = null;
        } else {
            futureResult = SettableFuture.create();
            resultList = new ArrayList<>(1);
            resultList.add(futureResult);
        }
    }

    @Override
    public void start() {
        if (subTasks.size() == 1) {
            // result list is bind to sub task results, no further action/listener required
            transportExecutor.execute(subTasks);
        } else if (subTasks.size() > 1) {
            assert futureResult != null;

            final List<ListenableFuture<TaskResult>> subTasksResult = transportExecutor.execute(subTasks);
            Futures.addCallback(Futures.allAsList(subTasksResult), new FutureCallback<List<TaskResult>>() {
                @Override
                public void onSuccess(List<TaskResult> results) {
                    assert results.size() == 1 : "Last sub-task is expected to have 1 result only";
                    futureResult.set(new RowCountResult(((Number) results.get(0).rows().iterator().next().get(0)).longValue()));
                }

                @Override
                public void onFailure(@Nonnull Throwable t) {
                    futureResult.setException(t);
                }
            });
        } // else: subTasks.size() = 0 -> immediate future already set in constructor
    }

    @Override
    public List<ListenableFuture<TaskResult>> result() {
        return resultList;
    }

    @Override
    public void upstreamResult(List<ListenableFuture<TaskResult>> result) {
        // should be only called for bulk update-by-query tasks where multiple UpsertTasks could exist
        resultList.addAll(result);
    }
}
