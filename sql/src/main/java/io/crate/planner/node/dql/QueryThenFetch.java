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

import io.crate.planner.PlanAndPlannedAnalyzedRelation;
import io.crate.planner.PlanVisitor;
import io.crate.planner.projection.Projection;
import io.crate.planner.symbol.Symbol;
import org.elasticsearch.common.Nullable;

import java.util.List;

public class QueryThenFetch extends PlanAndPlannedAnalyzedRelation {

    private final CollectNode collectNode;
    private @Nullable MergeNode mergeNode;
    private @Nullable List<Symbol> outputs;

    public QueryThenFetch(CollectNode collectNode, @Nullable MergeNode mergeNode) {
        this.collectNode = collectNode;
        this.mergeNode = mergeNode;
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

    /**
     * In some cases the QTF-Consumer doesn't add a mergeNode, so also
     * no FetchProjection is added. The FetchProjection will be created
     * by another consumer (e.g. CrossJoin).
     * In this cases only the outputs of the fetchProjection are stored.
     * @return a list of outputs which should be fetched by the fetchedProjection
     */
    @Nullable
    public List<Symbol> outputs() {
        return outputs;
    }

    public void outputs(List<Symbol> outputs) {
        this.outputs = outputs;
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
}
