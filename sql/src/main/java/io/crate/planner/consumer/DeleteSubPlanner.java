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

package io.crate.planner.consumer;

import io.crate.analyze.DeleteAnalyzedStatement;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.TableRelation;
import io.crate.metadata.Routing;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.IterablePlan;
import io.crate.planner.NoopPlan;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.node.dml.BulkDeletePlan;
import io.crate.planner.node.dml.DeleteByQueryNode;
import io.crate.planner.node.dml.DeletePlan;
import io.crate.planner.node.dml.ESDeleteNode;
import io.crate.planner.node.dql.MergeNode;
import org.elasticsearch.common.inject.Singleton;

import java.util.ArrayList;

@Singleton
public class DeleteSubPlanner {

    public Plan deletePlan(DeleteAnalyzedStatement statement, Planner.Context context) {
        TableRelation tableRelation = statement.analyzedRelation();

        ArrayList<Plan> childPlans = new ArrayList<>();
        for (WhereClause whereClause : statement.whereClauses()) {
            if (whereClause.noMatch()) {
                continue;
            }
            childPlans.add(deletePlan(tableRelation.tableInfo(), whereClause, context));
        }
        if (childPlans.isEmpty()) {
            return NoopPlan.INSTANCE;
        }
        if (childPlans.size() == 1) {
            return childPlans.get(0);
        }
        return new BulkDeletePlan(childPlans);
    }

    private Plan deletePlan(TableInfo tableInfo, WhereClause whereClause, Planner.Context context) {
        if (whereClause.docKeys().isPresent() && whereClause.docKeys().get().size()==1) {
            return deleteByIdPlan(tableInfo, whereClause);
        } else {
            return deleteByQueryPlan(tableInfo, whereClause, context);
        }
    }

    private Plan deleteByIdPlan(TableInfo tableInfo, WhereClause whereClause) {
        return new IterablePlan(new ESDeleteNode(tableInfo, whereClause.docKeys().get().getOnlyKey()));
    }

    private Plan deleteByQueryPlan(TableInfo tableInfo, WhereClause whereClause, Planner.Context context) {
        // TODO: optimize to delete-index on partitions

        Routing routing = tableInfo.getRouting(whereClause, null);
        DeleteByQueryNode deleteByQueryNode = new DeleteByQueryNode(context.nextExecutionNodeId(), routing, whereClause);

        MergeNode localMerge = MergeNode.mergeCount(
                context.nextExecutionNodeId(), "delete-by-query-merge", deleteByQueryNode);

        return new DeletePlan(deleteByQueryNode, localMerge);
    }
}
