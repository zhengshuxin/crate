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
import com.google.common.collect.ImmutableList;
import io.crate.analyze.WhereClause;
import io.crate.metadata.Routing;
import io.crate.planner.node.ExecutionNode;
import io.crate.planner.node.ExecutionNodeVisitor;
import io.crate.planner.symbol.Reference;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public class DeleteByQueryNode implements Streamable, ExecutionNode {

    public static final ExecutionNodeFactory<DeleteByQueryNode> FACTORY = new ExecutionNodeFactory<DeleteByQueryNode>() {
        @Override
        public DeleteByQueryNode create() {
            return new DeleteByQueryNode();
        }
    };

    private int executionNodeId;
    private Optional<UUID> jobId = Optional.absent();
    private Routing routing;
    private WhereClause whereClause = WhereClause.NO_MATCH;
    private Optional<Reference> routingColumn = Optional.absent();

    DeleteByQueryNode() {}

    public DeleteByQueryNode(StreamInput input) throws IOException {
        readFrom(input);
    }

    public DeleteByQueryNode(int executionNodeId,
                             Routing routing,
                             WhereClause whereClause,
                             @Nullable Reference routingColumn) {
        this.executionNodeId = executionNodeId;
        this.routing = routing;
        this.whereClause = whereClause;
        this.routingColumn = Optional.fromNullable(routingColumn);
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

    public Optional<Reference> routingColumn() {
        return routingColumn;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        executionNodeId = in.readVInt();
        routing = new Routing();
        routing.readFrom(in);
        whereClause = new WhereClause(in);
        if (in.readBoolean()) {
            jobId = Optional.of(new UUID(in.readLong(), in.readLong()));
        }
        if (in.readBoolean()) {
            routingColumn = Optional.of(Reference.fromStream(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(executionNodeId);
        routing.writeTo(out);
        whereClause.writeTo(out);
        out.writeBoolean(jobId.isPresent());
        if (jobId.isPresent()) {
            out.writeLong(jobId.get().getMostSignificantBits());
            out.writeLong(jobId.get().getLeastSignificantBits());
        }

        out.writeBoolean(routingColumn.isPresent());
        if (routingColumn.isPresent()) {
            Reference.toStream(routingColumn.get(), out);
        }
    }

    @Override
    public Type type() {
        return Type.DELETE_BY_QUERY;
    }

    @Override
    public String name() {
        return "delete-by-query";
    }

    @Override
    public int executionNodeId() {
        return executionNodeId;
    }

    @Override
    public Set<String> executionNodes() {
        return routing.nodes();
    }

    @Override
    public List<String> downstreamNodes() {
        return ImmutableList.of(ExecutionNode.DIRECT_RETURN_DOWNSTREAM_NODE);
    }

    @Override
    public int downstreamExecutionNodeId() {
        return ExecutionNode.NO_EXECUTION_NODE;
    }

    @Override
    public <C, R> R accept(ExecutionNodeVisitor<C, R> visitor, C context) {
        return visitor.visitDeleteByQueryNode(this, context);
    }
}
