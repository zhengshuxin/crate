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

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import io.crate.analyze.*;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.PlannedAnalyzedRelation;
import io.crate.analyze.relations.TableRelation;
import io.crate.exceptions.ValidationException;
import io.crate.planner.PlanNodeBuilder;
import io.crate.planner.node.NoopPlannedAnalyzedRelation;
import io.crate.planner.node.dql.join.NestedLoop;
import io.crate.planner.node.dql.join.NestedLoopNode;
import io.crate.planner.projection.Projection;
import io.crate.planner.symbol.Symbol;
import io.crate.sql.tree.QualifiedName;
import org.elasticsearch.cluster.ClusterService;

import java.util.*;


public class CrossJoinConsumer implements Consumer {

    private final Visitor visitor;

    public CrossJoinConsumer(ClusterService clusterService,
                             AnalysisMetaData analysisMetaData,
                             ConsumingPlanner consumingPlanner) {
        visitor = new Visitor(clusterService, analysisMetaData, consumingPlanner);
    }

    @Override
    public PlannedAnalyzedRelation consume(AnalyzedRelation rootRelation, ConsumerContext context) {
        return visitor.process(rootRelation, context);
    }

    private static class Visitor extends AnalyzedRelationVisitor<ConsumerContext, PlannedAnalyzedRelation> {

        private final ClusterService clusterService;
        private final AnalysisMetaData analysisMetaData;
        private final ConsumingPlanner consumingPlanner;

        public Visitor(ClusterService clusterService, AnalysisMetaData analysisMetaData, ConsumingPlanner consumingPlanner) {
            this.clusterService = clusterService;
            this.analysisMetaData = analysisMetaData;
            this.consumingPlanner = consumingPlanner;
        }

        @Override
        protected PlannedAnalyzedRelation visitAnalyzedRelation(AnalyzedRelation relation, ConsumerContext context) {
            return null;
        }

        @Override
        public PlannedAnalyzedRelation visitMultiSourceSelect(MultiSourceSelect statement, ConsumerContext context) {
            if (isUnsupportedStatement(statement, context)) return null;
            WhereClause where = MoreObjects.firstNonNull(statement.querySpec().where(), WhereClause.MATCH_ALL);
            if (where.noMatch()) {
                return new NoopPlannedAnalyzedRelation(statement);
            }

            final Map<Object, Integer> relationOrder = getRelationOrder(statement);
            List<QueriedTable> queriedTables = new ArrayList<>();
            for (Map.Entry<QualifiedName, AnalyzedRelation> entry : statement.sources().entrySet()) {
                AnalyzedRelation analyzedRelation = entry.getValue();
                if (!(analyzedRelation instanceof TableRelation)) {
                    context.validationException(new ValidationException("CROSS JOIN with sub queries is not supported"));
                    return null;
                }
                queriedTables.add(getQueriedTable(statement, entry.getKey(), (TableRelation) analyzedRelation));
            }
            sortQueriedTables(relationOrder, queriedTables);

            // TODO: replace references with docIds..

            return toNestedLoop(queriedTables, context);
        }

        private NestedLoop toNestedLoop(List<QueriedTable> queriedTables, ConsumerContext context) {
            Iterator<QueriedTable> iterator = queriedTables.iterator();
            NestedLoop nl = null;
            PlannedAnalyzedRelation left;
            PlannedAnalyzedRelation right;

            //int nestedLimit = pushDownLimit ? limit + offset : TopN.NO_LIMIT;
            //int nestedOffset = 0;

            while (iterator.hasNext()) {
                QueriedTable next = iterator.next();


                int currentLimit;
                int currentOffset;
                if (nl == null) {
                    assert iterator.hasNext();
                    QueriedTable second = iterator.next();
                    //currentLimit = iterator.hasNext() ? nestedLimit : limit;
                    //currentOffset = iterator.hasNext() ? nestedOffset : offset;

                    left = consumingPlanner.plan(next, context);
                    right = consumingPlanner.plan(second, context);
                    assert left != null && right != null;

                    /*
                    NestedLoopNode nestedLoopNode = PlanNodeBuilder.localNestedLoopNode(
                            ImmutableList.<Projection>of(),
                            Sets.newHashSet(clusterService.localNode().id()),
                            left.resultNode(),
                            right.resultNode(),
                            left.fields(),
                            right.fields(),
                            null,
                            null,
                            context.plannerContext()
                    );
                    nl = new NestedLoop(left, right, nestedLoopNode, true);
                    */
                    /*
                    nl.outputTypes(ImmutableList.<DataType>builder()
                            .addAll(left.outputTypes())
                            .addAll(right.outputTypes()).build());
                        */
                } else {
                    /*
                    currentLimit = iterator.hasNext() ? nestedLimit : limit;
                    currentOffset = iterator.hasNext() ? nestedOffset : offset;
                    */

                    /*
                    NestedLoop lastNL = nl;
                    right = consumingPlanner.plan(next, context);
                    assert right != null;
                    nl = new NestedLoop(lastNL, right, nestedLoopNode, true);
                    */
                    

                    /*
                    nl.outputTypes(ImmutableList.<DataType>builder()
                            .addAll(lastNL.outputTypes())
                            .addAll(right.outputTypes()).build());
                    */
                }
            }
            return nl;
        }

        private void sortQueriedTables(final Map<Object, Integer> relationOrder, List<QueriedTable> queriedTables) {
            Collections.sort(queriedTables, new Comparator<QueriedTable>() {
                @Override
                public int compare(QueriedTable o1, QueriedTable o2) {
                    return Integer.compare(
                            MoreObjects.firstNonNull(relationOrder.get(o1.tableRelation()), Integer.MAX_VALUE),
                            MoreObjects.firstNonNull(relationOrder.get(o2.tableRelation()), Integer.MAX_VALUE));
                }
            });
        }

        private QueriedTable getQueriedTable(MultiSourceSelect statement,
                                             QualifiedName relationName,
                                             TableRelation tableRelation) {
            QueriedTable queriedTable = QueriedTable.newSubRelation(relationName, tableRelation, statement.querySpec());
            queriedTable.normalize(analysisMetaData);
            return queriedTable;
        }

        /**
         * returns a map with the relation as keys and the values are their order in occurrence in the order by clause.
         *
         * e.g. select * from t1, t2, t3 order by t2.x, t3.y
         *
         * returns: {
         *     t2: 0                 (first)
         *     t3: 1                 (second)
         *     t1: Integer.MAX_VALUE (last)
         * }
         */
        private Map<Object, Integer> getRelationOrder(MultiSourceSelect statement) {
            OrderBy orderBy = statement.querySpec().orderBy();
            if (orderBy == null || !orderBy.isSorted()) {
                return Collections.emptyMap();
            }

            final Map<Object, Integer> orderByOrder = new IdentityHashMap<>();
            int idx = 0;
            for (Symbol orderBySymbol : orderBy.orderBySymbols()) {
                for (AnalyzedRelation analyzedRelation : statement.sources().values()) {
                    QuerySplitter.RelationCount relationCount = QuerySplitter.getRelationCount(analyzedRelation, orderBySymbol);
                    if (relationCount != null && relationCount.numOther == 0 && relationCount.numThis > 0) {
                        orderByOrder.put(analyzedRelation, idx);
                    } else {
                        orderByOrder.put(analyzedRelation, Integer.MAX_VALUE);
                    }
                }
                idx++;
            }
            return orderByOrder;
        }

        private boolean isUnsupportedStatement(MultiSourceSelect statement, ConsumerContext context) {
            if (statement.sources().size() < 2) {
                return true;
            }

            List<Symbol> groupBy = statement.querySpec().groupBy();
            if (groupBy != null && !groupBy.isEmpty()) {
                context.validationException(new ValidationException("GROUP BY on CROSS JOIN is not supported"));
                return true;
            }
            if (statement.querySpec().hasAggregates()) {
                context.validationException(new ValidationException("AGGREGATIONS on CROSS JOIN is not supported"));
                return true;
            }
            return false;
        }
    }
}
