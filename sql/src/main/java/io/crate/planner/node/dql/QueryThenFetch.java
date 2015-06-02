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

package io.crate.planner.node.dql;

import io.crate.analyze.OrderBy;
import io.crate.metadata.ReferenceInfo;
import io.crate.planner.PlanAndPlannedAnalyzedRelation;
import io.crate.planner.PlanVisitor;
import io.crate.planner.projection.Projection;
import io.crate.planner.symbol.Field;
import io.crate.planner.symbol.Symbol;
import org.elasticsearch.common.Nullable;

import java.util.List;

public class QueryThenFetch extends PlanAndPlannedAnalyzedRelation {

    private final CollectNode collectNode;
    private @Nullable MergeNode mergeNode;
    private final Context context;

    public QueryThenFetch(CollectNode collectNode, @Nullable MergeNode mergeNode, Context context) {
        this.collectNode = collectNode;
        this.mergeNode = mergeNode;
        this.context = context;
    }

    public CollectNode collectNode() {
        return collectNode;
    }

    @Nullable
    public MergeNode mergeNode() {
        return mergeNode;
    }

    public void mergeNode(MergeNode mergeNode) {
        this.mergeNode = mergeNode;
    }

    public Context context() {
        return context;
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
        return visitor.visitQueryThenFetch(this, context);
    }

    @Override
    public void addProjection(Projection projection) {
        resultNode().addProjection(projection);
    }

    @Override
    public boolean resultIsDistributed() {
        return mergeNode == null;
    }

    @Override
    public DQLPlanNode resultNode() {
        return mergeNode == null ? collectNode : mergeNode;
    }

    public static class Context {

        private final List<Symbol> outputs;
        private final List<Symbol> collectSymbols;
        private final List<ReferenceInfo> partitionedByColumns;
        private final List<Field> fields;
        @Nullable
        private final OrderBy orderBy;

        public Context(List<Symbol> outputs,
                       List<Symbol> collectSymbols,
                       @Nullable OrderBy orderBy,
                       List<ReferenceInfo> partitionedByColumns,
                       List<Field> fields) {
            this.outputs = outputs;
            this.partitionedByColumns = partitionedByColumns;
            this.collectSymbols = collectSymbols;
            this.orderBy = orderBy;
            this.fields = fields;
        }

        public List<Symbol> outputs() {
            return outputs;
        }

        public List<Symbol> collectSymbols() {
            return collectSymbols;
        }

        public List<ReferenceInfo> partitionedByColumns() {
            return partitionedByColumns;
        }

        public OrderBy orderBy() {
            return orderBy;
        }

        public List<Field> fields() {
            return fields;
        }

    }
}
