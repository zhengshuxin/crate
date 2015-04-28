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
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessorResponse;
import org.elasticsearch.action.bulk.BulkResponse;

import java.util.Arrays;
import java.util.List;

public class BulkDeleteResponse implements BulkProcessorResponse<BulkItemResponse> {

    private final BulkItemResponse[] bulkResponses;
    private final IntArrayList itemIndices;

    public BulkDeleteResponse(BulkResponse bulkResponse) {
        this.bulkResponses = bulkResponse.getItems();
        this.itemIndices = new IntArrayList(bulkResponse.getItems().length);
        for (int i = 0; i < bulkResponses.length; i++) {
            BulkItemResponse bulkItemResponse = bulkResponses[i];
            if (bulkItemResponse.isFailed()) {
                bulkResponses[i] = null;
            } else {
                itemIndices.add(i);
            }
        }
    }

    @Override
    public IntArrayList itemIndices() {
        return itemIndices;
    }

    @Override
    public List<BulkItemResponse> responses() {
        return Arrays.asList(bulkResponses);
    }
}
