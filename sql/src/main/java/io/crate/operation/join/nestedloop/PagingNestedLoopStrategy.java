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

package io.crate.operation.join.nestedloop;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.FutureCallback;
import io.crate.core.bigarray.IterableBigArray;
import io.crate.core.bigarray.MultiNativeArrayBigArray;
import io.crate.executor.Page;
import io.crate.executor.PageInfo;
import io.crate.executor.PageableTaskResult;
import io.crate.executor.TaskResult;
import io.crate.operation.projectors.Projector;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * true paging for nested loop, only loading a single page at a time
 * into ram for the outer relation.
 *
 * Only possible, when:
 *
 *  * NestedLoopNode has no projections
 *
 * This brave strategy keeps track of the offset and pagelimit itself
 */
class PagingNestedLoopStrategy implements NestedLoopStrategy {

    private final NestedLoopOperation nestedLoopOperation;
    private final NestedLoopExecutorService nestedLoopExecutorService;

    public PagingNestedLoopStrategy(NestedLoopOperation nestedLoopOperation,
                                    NestedLoopExecutorService nestedLoopExecutorService) {
        this.nestedLoopOperation = nestedLoopOperation;
        this.nestedLoopExecutorService = nestedLoopExecutorService;
    }

    @Override
    public TaskResult emptyResult() {
        return PageableTaskResult.EMPTY_PAGEABLE_RESULT;
    }

    @Override
    public int rowsToProduce(Optional<PageInfo> pageInfo) {
        assert pageInfo.isPresent() : "pageInfo is not present for " + name();
        return pageInfo.get().position() + pageInfo.get().size();
    }

    @Override
    public void onFirstJoin(JoinContext joinContext) {
        // do nothing
    }

    @Override
    public TaskResult produceFirstResult(Object[][] rows, Optional<PageInfo> pageInfo, JoinContext joinContext) {
        assert pageInfo.isPresent() : "pageInfo is not present for " + name();
        IterableBigArray<Object[]> wrappedRows = new MultiNativeArrayBigArray<Object[]>(0, rows.length, rows);
        return new NestedLoopOperation.NestedLoopPageableTaskResult(nestedLoopOperation, wrappedRows, pageInfo.get().position(), pageInfo.get(), joinContext);
    }

    @Override
    public String name() {
        return "optimized paging";
    }

    @Override
    public NestedLoopExecutor executor(JoinContext ctx, Optional<PageInfo> pageInfo, Projector downstream, FutureCallback<Void> callback) {
        assert pageInfo.isPresent() : "pageInfo is not present for " + name();
        int rowsToProduce = pageInfo.get().size();
        int rowsToSkip = 0;
        if (ctx.inFirstIteration()) {
            rowsToProduce += pageInfo.get().position(); // only produce pageInfo offset on first page
            rowsToSkip += nestedLoopOperation.offset();
        }
        return new PagingExecutor(ctx, rowsToProduce, rowsToSkip, nestedLoopExecutorService.executor(), downstream, callback);
    }

    static class PagingExecutor implements NestedLoopExecutor {
        private final JoinContext ctx;
        private final FutureCallback<Void> callback;
        private final Executor nestedLoopExecutor;

        /**
         * handle the pagelimit and offset ourselves
         */
        private final AtomicInteger rowsToProduce;
        private final AtomicInteger rowsToSkip;

        private final Projector downstream;

        public PagingExecutor(JoinContext ctx,
                               int rowsToProduce,
                               int rowsToSkip,
                               Executor nestedLoopExecutor,
                               Projector downstream,
                               FutureCallback<Void> finalCallback) {
            this.ctx = ctx;
            this.rowsToProduce = new AtomicInteger(rowsToProduce);
            this.rowsToSkip = new AtomicInteger(rowsToSkip);
            this.callback = finalCallback;
            this.nestedLoopExecutor = nestedLoopExecutor;
            this.downstream = downstream;
        }


        @Override
        public void startExecution() {
            // assume taskresults with pages are already there
            Page outerPage = ctx.outerTaskResult.page();
            Outer:
            while (outerPage.size() > 0) {
                Page innerPage = ctx.innerTaskResult.page();
                try {
                    while (innerPage.size() > 0) {

                        ctx.outerPageIterator = outerPage.iterator();
                        ctx.innerPageIterator = innerPage.iterator();

                        // set outer row
                        if (!ctx.advanceOuterRow()) {
                            // outer page is empty
                            callback.onSuccess(null);
                        }

                        boolean wantMore = joinPages();
                        if (!wantMore) {
                            callback.onSuccess(null);
                            break Outer;
                        }
                        innerPage = ctx.fetchInnerPage(ctx.advanceInnerPageInfo());
                    }
                    ctx.fetchInnerPage(ctx.resetInnerPageInfo()); // reset inner iterator
                    outerPage = ctx.fetchOuterPage(ctx.advanceOuterPageInfo());
                } catch (InterruptedException | ExecutionException e) {
                    callback.onFailure(e);
                    return;
                }
            }
            callback.onSuccess(null);
        }

        private boolean joinPages() {
            boolean wantMore = true;
            Object[] innerRow;

            Outer:
            do {
                while (ctx.innerPageIterator.hasNext()) {
                    innerRow = ctx.innerPageIterator.next();
                    if (rowsToSkip.get() > 0) {
                        rowsToSkip.decrementAndGet();
                        continue;
                    }

                    wantMore = this.downstream.setNextRow(
                            ctx.combine(ctx.outerRow(), innerRow)
                    );
                    wantMore = wantMore && rowsToProduce.decrementAndGet() > 0;
                    if (!wantMore) {
                        break Outer;
                    }
                }
                // reset inner iterator
                ctx.innerPageIterator = ctx.innerTaskResult.page().iterator();
            } while (ctx.advanceOuterRow());
            return wantMore;
        }

        @Override
        public void carryOnExecution() {
            boolean wantMore = joinPages();
            if (wantMore) {
                try {
                    if (ctx.outerTaskResult.page().size() > 0) {
                        ctx.fetchInnerPage(ctx.advanceInnerPageInfo());
                    } else {
                        ctx.fetchOuterPage(ctx.advanceOuterPageInfo());
                    }
                    startExecution();
                } catch (InterruptedException | ExecutionException e) {
                    callback.onFailure(e);
                }
            } else {
                callback.onSuccess(null);
            }
        }
    }
}
