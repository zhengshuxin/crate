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
import io.crate.Constants;
import io.crate.analyze.*;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.PlannedAnalyzedRelation;
import io.crate.analyze.relations.TableRelation;
import io.crate.exceptions.ValidationException;
import io.crate.planner.PlanAndPlannedAnalyzedRelation;
import io.crate.planner.PlanNodeBuilder;
import io.crate.planner.Planner;
import io.crate.planner.node.NoopPlannedAnalyzedRelation;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.node.dql.MergeNode;
import io.crate.planner.node.dql.QueryThenFetch;
import io.crate.planner.node.dql.join.NestedLoop;
import io.crate.planner.node.dql.join.NestedLoopNode;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.projection.builder.ProjectionBuilder;
import io.crate.planner.symbol.*;
import io.crate.sql.tree.QualifiedName;

import java.util.*;


public class CrossJoinConsumer implements Consumer {

    private final CrossJoinVisitor visitor;
    private final static InputColumnProducer INPUT_COLUMN_PRODUCER = new InputColumnProducer();

    public CrossJoinConsumer() {
        visitor = new CrossJoinVisitor();
    }

    @Override
    public boolean consume(AnalyzedRelation rootRelation, ConsumerContext context) {
        AnalyzedRelation analyzedRelation = visitor.process(rootRelation, context);
        if (analyzedRelation != null) {
            context.rootRelation(analyzedRelation);
            return true;
        }
        return false;
    }

    private static class CrossJoinVisitor extends AnalyzedRelationVisitor<ConsumerContext, PlannedAnalyzedRelation> {

        @Override
        protected PlannedAnalyzedRelation visitAnalyzedRelation(AnalyzedRelation relation, ConsumerContext context) {
            return null;
        }

        @Override
        public PlannedAnalyzedRelation visitMultiSourceSelect(MultiSourceSelect statement, ConsumerContext context) {
            if (statement.sources().size() < 2) {
                return null;
            }

            List<Symbol> groupBy = statement.querySpec().groupBy();
            if (groupBy != null && !groupBy.isEmpty()) {
                context.validationException(new ValidationException("GROUP BY on CROSS JOIN is not supported"));
                return null;
            }
            if (statement.querySpec().hasAggregates()) {
                context.validationException(new ValidationException("AGGREGATIONS on CROSS JOIN is not supported"));
                return null;
            }

            WhereClause where = MoreObjects.firstNonNull(statement.querySpec().where(), WhereClause.MATCH_ALL);
            if (where.noMatch()) {
                return new NoopPlannedAnalyzedRelation(statement);
            }

            // check that every inner relation is planned
            List<Symbol> allCollectorOutputs = new ArrayList<>();
            for (AnalyzedRelation relation : statement.sources().values()) {
                if (relation instanceof PlannedAnalyzedRelation) {
                    if (relation instanceof QueryThenFetch) {
                        allCollectorOutputs.addAll(((QueryThenFetch) relation).collectNode().toCollect());
                    } else if (relation instanceof NoopPlannedAnalyzedRelation) {
                        return (NoopPlannedAnalyzedRelation)relation;
                    }
                } else {
                    return null;
                }
            }

            // TODO: check remaining query
            // TODO: consider orderByOrder
            ProjectionBuilder projectionBuilder = new ProjectionBuilder(statement.querySpec());

            PlanAndPlannedAnalyzedRelation left = null;
            PlanAndPlannedAnalyzedRelation right = null;
            MergeNode leftMergeNode = null;
            MergeNode rightMergeNode = null;
            Iterator<AnalyzedRelation> iterator = statement.sources().values().iterator();
            while (iterator.hasNext()) {
                PlanAndPlannedAnalyzedRelation plannedAnalyzedRelation = (PlanAndPlannedAnalyzedRelation)iterator.next();
                if (left == null) {
                    assert iterator.hasNext();
                    PlanAndPlannedAnalyzedRelation secondAnalyzedRelation = (PlanAndPlannedAnalyzedRelation)iterator.next();
                    List<Projection> projections = ImmutableList.of();
                    leftMergeNode = createMergeNode(projections, plannedAnalyzedRelation, context.plannerContext());
                    rightMergeNode = createMergeNode(projections, secondAnalyzedRelation, context.plannerContext());

                    left = plannedAnalyzedRelation;
                    right = secondAnalyzedRelation;
                } else {
                    // have to create an inner Plan
                    NestedLoopNode nestedLoopNode = PlanNodeBuilder.nestedLoopNode(
                            ImmutableList.<Projection>of(),
                            leftMergeNode,
                            rightMergeNode,
                            context.plannerContext()
                    );
                    nestedLoopNode.executionNodes(new HashSet<String>(Collections.singletonList(context.plannerContext().clusterService().localNode().id())));
                    NestedLoop nl = new NestedLoop(left, right, nestedLoopNode, true);
                    left = nl;

                    leftMergeNode = null;
                    right = plannedAnalyzedRelation;
                    rightMergeNode = createMergeNode(ImmutableList.<Projection>of(), right, context.plannerContext());
                }
            }

            int rootLimit = MoreObjects.firstNonNull(statement.querySpec().limit(), Constants.DEFAULT_SELECT_LIMIT);
            TopNProjection topNProjection = projectionBuilder.topNProjection(
                    allCollectorOutputs,
                    null,
                    statement.querySpec().offset(),
                    rootLimit,
                    null);
            // TODO: put outputs in correct order
            topNProjection.outputs(allCollectorOutputs);

            NestedLoopNode nestedLoopNode = PlanNodeBuilder.nestedLoopNode(
                    ImmutableList.<Projection>of(topNProjection),
                    leftMergeNode,
                    rightMergeNode,
                    context.plannerContext()
            );
            nestedLoopNode.executionNodes(new HashSet<String>(Collections.singletonList(context.plannerContext().clusterService().localNode().id())));

            // TODO: find a generic solution
            if (left instanceof NestedLoop) {
                ((NestedLoop) left).nestedLoopNode().downstreamExecutionNodeId(nestedLoopNode.executionNodeId());
            }

            // set fieldOutputs to the lastNestedLoopNode so the ordering of the outputs is correct
            nestedLoopNode.outputTypes(Symbols.extractTypes(statement.fields()));
            NestedLoop plan = new NestedLoop(left, right, nestedLoopNode, true);

            MergeNode localMergeNode;
            OrderBy orderBy = statement.querySpec().orderBy();
            if (orderBy != null && orderBy.isSorted()) {
                localMergeNode = PlanNodeBuilder.sortedLocalMerge(
                        ImmutableList.<Projection>of(),
                        orderBy,
                        replaceFieldsWithInputColumns(orderBy.orderBySymbols(), allCollectorOutputs),
                        null,
                        plan.resultNode(),
                        context.plannerContext());
            } else {
                localMergeNode = PlanNodeBuilder.localMerge(
                        ImmutableList.<Projection>of(),
                        plan.resultNode(),
                        context.plannerContext());
            }
            localMergeNode.executionNodes(new HashSet<String>(Collections.singletonList(context.plannerContext().clusterService().localNode().id())));

            nestedLoopNode.downstreamExecutionNodeId(localMergeNode.executionNodeId());
            nestedLoopNode.downstreamNodes(localMergeNode.executionNodes());
            plan.localMergeNode(localMergeNode);
            return plan;
        }

        @Override
        public PlannedAnalyzedRelation visitInsertFromQuery(InsertFromSubQueryAnalyzedStatement insertFromSubQueryAnalyzedStatement, ConsumerContext context) {
            InsertFromSubQueryConsumer.planInnerRelation(insertFromSubQueryAnalyzedStatement, context, this);
            return null;
        }

        private MergeNode createMergeNode(List<Projection> projections, PlannedAnalyzedRelation previous, Planner.Context context) {
            MergeNode mergeNode;
            // TODO: make this more generic
            if(previous instanceof QueryThenFetch && ((QueryThenFetch) previous).context().orderBy() != null) {
                QueryThenFetch.Context ctx = ((QueryThenFetch) previous).context();
                mergeNode = PlanNodeBuilder.sortedLocalMerge(
                        projections,
                        ctx.orderBy(),
                        ctx.collectSymbols(),
                        null,
                        previous.resultNode(),
                        context
                        );
            } else {
                mergeNode = PlanNodeBuilder.localMerge(projections, previous.resultNode(), context);
            }
            mergeNode.executionNodes(new HashSet<String>(Collections.singletonList(context.clusterService().localNode().id())));
            if(previous.resultNode() instanceof CollectNode) {
                ((CollectNode)previous.resultNode()).downstreamNodes(Collections.singletonList(context.clusterService().localNode().id()));
                ((CollectNode) previous.resultNode()).downstreamExecutionNodeId(mergeNode.executionNodeId());
            }
            return mergeNode;
        }

        /**
         * generates new symbols that will use InputColumn symbols to point to the output of the given relations
         *
         * @param statementOutputs: [ u1.id,  add(u1.id, u2.id) ]
         * @param inputSymbols:
         * {
         *     [ u1.id, u2.id ],
         * }
         *
         * @return [ in(0), add( in(0), in(1) ) ]
         */
        private List<Symbol> replaceFieldsWithInputColumns(Collection<? extends Symbol> statementOutputs,
                                                           List<Symbol> inputSymbols) {
            List<Symbol> result = new ArrayList<>();
            for (Symbol statementOutput : statementOutputs) {
                result.add(replaceFieldsWithInputColumns(statementOutput, inputSymbols));
            }
            return result;
        }

        private Symbol replaceFieldsWithInputColumns(Symbol symbol, List<Symbol> inputSymbols) {
            return INPUT_COLUMN_PRODUCER.process(symbol, new InputColumnProducerContext(inputSymbols));
        }

    }

    private static class InputColumnProducerContext {

        private List<Symbol> inputs;

        public InputColumnProducerContext(List<Symbol> inputs) {
            this.inputs = inputs;
        }
    }

    private static class InputColumnProducer extends SymbolVisitor<InputColumnProducerContext, Symbol> {

        @Override
        public Symbol visitFunction(Function function, InputColumnProducerContext context) {
            int idx = 0;
            for (Symbol input : context.inputs) {
                if (input.equals(function)) {
                    return new InputColumn(idx, input.valueType());
                }
                idx++;
            }
            List<Symbol> newArgs = new ArrayList<>(function.arguments().size());
            for (Symbol argument : function.arguments()) {
                newArgs.add(process(argument, context));
            }
            return new Function(function.info(), newArgs);
        }

        @Override
        public Symbol visitField(Field field, InputColumnProducerContext context) {
            int idx = 0;
            for (Symbol input : context.inputs) {
                if (input.equals(field)) {
                    return new InputColumn(idx, input.valueType());
                }
                idx++;
            }
            return field;
        }

        @Override
        public Symbol visitLiteral(Literal literal, InputColumnProducerContext context) {
            return literal;
        }
    }


    public static <R, C> void planInnerRelations(MultiSourceSelect statement, C context, AnalyzedRelationVisitor<C, R> visitor, AnalysisMetaData analysisMetaData) throws ValidationException{
        if (statement.sources().size() < 2) {
            return;
        }

        List<Symbol> groupBy = statement.querySpec().groupBy();
        if (groupBy != null && !groupBy.isEmpty()) {
            throw new ValidationException("GROUP BY on CROSS JOIN is not supported");
        }
        if (statement.querySpec().hasAggregates()) {
            throw new ValidationException("AGGREGATIONS on CROSS JOIN is not supported");
        }

        WhereClause where = MoreObjects.firstNonNull(statement.querySpec().where(), WhereClause.MATCH_ALL);
        if (where.noMatch()) {
            return;
        }

        for (Map.Entry<QualifiedName, AnalyzedRelation> entry : statement.sources().entrySet()) {
            AnalyzedRelation analyzedRelation = entry.getValue();
            // relation is already planned
            if (analyzedRelation instanceof PlannedAnalyzedRelation) {
                continue;
            }
            if (!(analyzedRelation instanceof TableRelation)) {
                throw new ValidationException("CROSS JOIN with sub queries is not supported");
            }
            TableRelation tableRelation = (TableRelation) analyzedRelation;
            final QueriedTable queriedTable = QueriedTable.newSubRelation(entry.getKey(), tableRelation, statement.querySpec());
            queriedTable.normalize(analysisMetaData);

            R innerRelation = visitor.process(queriedTable, context);
            if (innerRelation != null && innerRelation instanceof PlannedAnalyzedRelation) {
                statement.sources().put(entry.getKey(), (PlannedAnalyzedRelation)innerRelation);
            } else {
                statement.sources().put(entry.getKey(), queriedTable);
            }

        }
    }

}
