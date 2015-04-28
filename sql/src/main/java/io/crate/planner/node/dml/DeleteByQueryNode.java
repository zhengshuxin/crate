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

package io.crate.planner.node.dml;

import com.google.common.base.Optional;
import io.crate.analyze.WhereClause;
import io.crate.metadata.Routing;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;
import java.util.UUID;

/**
 * TODO: implement ExecutionNode when available
 */
public class DeleteByQueryNode implements Streamable {

    private Optional<UUID> jobId = Optional.absent();
    private Routing routing;
    private WhereClause whereClause = WhereClause.NO_MATCH;

    public DeleteByQueryNode(StreamInput in) throws IOException {
        readFrom(in);
    }

    public DeleteByQueryNode(Routing routing, WhereClause whereClause) {
        this.routing = routing;
        this.whereClause = whereClause;
    }

    public void jobId(UUID jobId) {
        this.jobId = Optional.of(jobId);
    }

    public Optional<UUID> jobId() {
        return jobId;
    }

    public Routing routing() {
        return routing;
    }

    public WhereClause whereClause() {
        return whereClause;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        routing = new Routing();
        routing.readFrom(in);
        whereClause = new WhereClause(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        routing.writeTo(out);
        whereClause.writeTo(out);
    }
}
