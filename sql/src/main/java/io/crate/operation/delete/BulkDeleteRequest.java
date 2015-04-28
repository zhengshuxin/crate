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

import com.carrotsearch.hppc.IntArrayList;
import com.google.common.base.MoreObjects;
import io.crate.Constants;
import io.crate.planner.symbol.Symbol;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.index.shard.ShardId;

import javax.annotation.Nullable;
import java.util.concurrent.atomic.AtomicInteger;

public class BulkDeleteRequest implements BulkProcessorRequest {

    private final BulkRequest bulkRequest;
    private final IntArrayList itemIndices;
    private final AtomicInteger counter;

    public BulkDeleteRequest() {
        this.bulkRequest = new BulkRequest();
        this.itemIndices = new IntArrayList();
        this.counter = new AtomicInteger(0);
    }

    @Override
    public IntArrayList itemIndices() {
        return itemIndices;
    }

    public BulkRequest bulkRequest() {
        return bulkRequest;
    }

    public BulkDeleteRequest add(DeleteRequest deleteRequest) {
        bulkRequest.add(deleteRequest);
        itemIndices.add(counter.getAndIncrement());
        return this;
    }

    public static class Builder implements SymbolBasedBulkShardProcessor.BulkRequestBuilder<BulkDeleteRequest> {

        @Override
        public BulkDeleteRequest newRequest(ShardId shardId) {
            return new BulkDeleteRequest();
        }

        @Override
        public void addItem(BulkDeleteRequest existingRequest, ShardId shardId, int location, String id, @Nullable Symbol[] assignments, @Nullable Object[] missingAssignments, @Nullable String routing, @Nullable Long version) {
            existingRequest.add(
                    new DeleteRequest(
                            shardId.getIndex(),
                            Constants.DEFAULT_MAPPING_TYPE,
                            id)
                            .refresh(false)
                            .routing(routing)
                            .version(MoreObjects.firstNonNull(version, Versions.MATCH_ANY))
            );
        }
    }

    public static class Executor implements BulkRequestExecutor<BulkDeleteRequest, BulkDeleteResponse> {

        private final TransportBulkAction transportBulkAction;

        public Executor(TransportBulkAction transportBulkAction) {
            this.transportBulkAction = transportBulkAction;
        }

        @Override
        public void execute(BulkDeleteRequest request, final ActionListener<BulkDeleteResponse> listener) {
            this.transportBulkAction.execute(request.bulkRequest(), new ActionListener<BulkResponse>() {
                @Override
                public void onResponse(BulkResponse bulkItemResponses) {
                    listener.onResponse(new BulkDeleteResponse(bulkItemResponses));
                }

                @Override
                public void onFailure(Throwable e) {
                    listener.onFailure(e);
                }
            });
        }
    }

}
