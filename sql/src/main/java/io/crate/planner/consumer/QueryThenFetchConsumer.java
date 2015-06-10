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
import io.crate.exceptions.ValidationException;
import io.crate.exceptions.VersionInvalidException;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.DocReferenceConverter;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.ScoreReferenceDetector;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.projectors.FetchProjector;
import io.crate.planner.PlanNodeBuilder;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.NoopPlannedAnalyzedRelation;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.node.dql.MergeNode;
import io.crate.planner.node.dql.QueryThenFetch;
import io.crate.planner.projection.FetchProjection;
import io.crate.planner.projection.MergeProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.projection.builder.ProjectionBuilder;
import io.crate.planner.projection.builder.SplitPoints;
import io.crate.planner.symbol.*;
import io.crate.types.DataTypes;

import java.util.*;

public class QueryThenFetchConsumer implements Consumer {

    private static final OutputOrderReferenceCollector OUTPUT_ORDER_REFERENCE_COLLECTOR = new OutputOrderReferenceCollector();
    private static final ReferencesCollector REFERENCES_COLLECTOR = new ReferencesCollector();
    private static final ScoreReferenceDetector SCORE_REFERENCE_DETECTOR = new ScoreReferenceDetector();
    private static final ColumnIdent DOC_ID_COLUMN_IDENT = new ColumnIdent(DocSysColumns.DOCID.name());
    private static final InputColumn DEFAULT_DOC_ID_INPUT_COLUMN = new InputColumn(0, DataTypes.STRING);
    private final Visitor visitor;

    public QueryThenFetchConsumer(AnalysisMetaData analysisMetaData) {
        this.visitor = new Visitor(analysisMetaData);
    }

    @Override
    public boolean consume(AnalyzedRelation rootRelation, ConsumerContext context) {
        Context ctx = new Context(context);
        context.rootRelation(visitor.process(context.rootRelation(), ctx));
        return ctx.result;
    }

    private static class Context {

        private final ConsumerContext consumerContext;
        boolean result = false;

        public Context(ConsumerContext consumerContext) {
            this.consumerContext = consumerContext;
        }

        public ConsumerContext consumerContext() {
            return consumerContext;
        }

        public void result(boolean result) {
            this.result = result;
        }
    }

    private static class Visitor extends AnalyzedRelationVisitor<Context, AnalyzedRelation> {

        private final AnalysisMetaData analysisMetaData;

        public Visitor(AnalysisMetaData analysisMetaData) {
            this.analysisMetaData = analysisMetaData;
        }

        @Override
        public AnalyzedRelation visitQueriedTable(QueriedTable table, Context context) {
            QuerySpec querySpec = table.querySpec();
            if (querySpec.hasAggregates() || querySpec.groupBy()!=null) {
                return table;
            }
            TableInfo tableInfo = table.tableRelation().tableInfo();
            if (tableInfo.schemaInfo().systemSchema() || tableInfo.rowGranularity() != RowGranularity.DOC) {
                return table;
            }

            if(querySpec.where().hasVersions()){
                context.consumerContext().validationException(new VersionInvalidException());
                return table;
            }

            if (querySpec.where().noMatch()) {
                context.result(true);
                return new NoopPlannedAnalyzedRelation(table);
            }

            boolean outputsAreAllOrdered = false;
            boolean needFetchProjection = REFERENCES_COLLECTOR.collect(querySpec.outputs()).containsAnyReference();
            List<Projection> collectProjections = new ArrayList<>();
            List<Projection> mergeProjections = new ArrayList<>();
            List<Symbol> collectSymbols = new ArrayList<>();
            List<Symbol> outputSymbols = new ArrayList<>();
            ReferenceInfo docIdRefInfo = tableInfo.getReferenceInfo(DOC_ID_COLUMN_IDENT);

            ProjectionBuilder projectionBuilder = new ProjectionBuilder(querySpec);
            SplitPoints splitPoints = projectionBuilder.getSplitPoints();

            // MAP/COLLECT related
            OrderBy orderBy = querySpec.orderBy();
            if (orderBy != null) {
                table.tableRelation().validateOrderBy(orderBy);

                // detect if all output columns are used in orderBy,
                // if so, no fetch projection is needed
                // TODO: if no dedicated fetchPhase is needed we should stick to QAF instead
                OutputOrderReferenceContext outputOrderContext =
                        OUTPUT_ORDER_REFERENCE_COLLECTOR.collect(splitPoints.leaves());
                outputOrderContext.collectOrderBy = true;
                OUTPUT_ORDER_REFERENCE_COLLECTOR.collect(orderBy.orderBySymbols(), outputOrderContext);
                outputsAreAllOrdered = outputOrderContext.outputsAreAllOrdered();
                if (outputsAreAllOrdered) {
                    collectSymbols = splitPoints.toCollect();
                } else {
                    collectSymbols.addAll(orderBy.orderBySymbols());
                }
            }

            needFetchProjection = needFetchProjection & !outputsAreAllOrdered;

            if (needFetchProjection) {
                collectSymbols.add(0, new Reference(docIdRefInfo));
                for (Symbol symbol : querySpec.outputs()) {
                    // _score can only be resolved during collect
                    if (SCORE_REFERENCE_DETECTOR.detect(symbol) && !collectSymbols.contains(symbol)) {
                        collectSymbols.add(symbol);
                    }
                    outputSymbols.add(DocReferenceConverter.convertIfPossible(symbol, tableInfo));
                }
            } else {
                // no fetch projection needed, resolve all symbols during collect
                collectSymbols = splitPoints.toCollect();
            }
            if (orderBy != null) {
                MergeProjection mergeProjection = projectionBuilder.mergeProjection(
                        collectSymbols,
                        orderBy);
                collectProjections.add(mergeProjection);
            }

            final CollectNode collectNode = PlanNodeBuilder.collect(
                    tableInfo,
                    context.consumerContext().plannerContext(),
                    querySpec.where(),
                    collectSymbols,
                    ImmutableList.<Projection>of(),
                    orderBy,
                    MoreObjects.firstNonNull(querySpec.limit(), Constants.DEFAULT_SELECT_LIMIT) + querySpec.offset()
            );


            collectNode.keepContextForFetcher(needFetchProjection);
            collectNode.projections(collectProjections);
            // If this is not the root relation return a QTF-Node which contains only a collectNode
            // MergeNode has to be created by the parent relation
            QueryThenFetch.Context qtfContext = new QueryThenFetch.Context(
                    outputSymbols,
                    collectSymbols,
                    orderBy,
                    tableInfo.partitionedByColumns());
            if (context.consumerContext().rootRelation() != table) {
                QueryThenFetch qtf = new QueryThenFetch(collectNode, null, qtfContext);
                return qtf;
            }
            // MAP/COLLECT related END

            // HANDLER/MERGE/FETCH related
            TopNProjection topNProjection;
            if (needFetchProjection) {
                topNProjection = projectionBuilder.topNProjection(
                        collectSymbols,
                        null,
                        querySpec.offset(),
                        querySpec.limit(),
                        null);
                mergeProjections.add(topNProjection);

                // by default don't split fetch requests into pages/chunks,
                // only if record set is higher than default limit
                int bulkSize = FetchProjector.NO_BULK_REQUESTS;
                if (topNProjection.limit() > Constants.DEFAULT_SELECT_LIMIT) {
                    bulkSize = Constants.DEFAULT_SELECT_LIMIT;
                }

                Map<Integer, ArrayList<String>> nodeIds;

                // TODO: create FetchProjectionBuilder
                FetchProjection fetchProjection = new FetchProjection(
                        context.consumerContext().plannerContext().jobSearchContextIdToExecutionNodeId(),
                        DEFAULT_DOC_ID_INPUT_COLUMN, collectSymbols, outputSymbols,
                        tableInfo.partitionedByColumns(),
                        new HashMap<Integer, List<String>>(){{
                            put(collectNode.executionNodeId(), new ArrayList<>(collectNode.executionNodes()));}},
                        bulkSize,
                        querySpec.isLimited(),
                        context.consumerContext().plannerContext().jobSearchContextIdToNode(),
                        context.consumerContext().plannerContext().jobSearchContextIdToShard()
                );
                mergeProjections.add(fetchProjection);
            } else {
                topNProjection = projectionBuilder.topNProjection(
                        collectSymbols,
                        null,
                        querySpec.offset(),
                        querySpec.limit(),
                        querySpec.outputs());
                mergeProjections.add(topNProjection);
            }

            MergeNode localMergeNode;
            if (orderBy != null) {
                localMergeNode = PlanNodeBuilder.sortedLocalMerge(
                        mergeProjections,
                        orderBy,
                        collectSymbols,
                        null,
                        collectNode,
                        context.consumerContext().plannerContext());
            } else {
                localMergeNode = PlanNodeBuilder.localMerge(
                        mergeProjections,
                        collectNode,
                        context.consumerContext().plannerContext());
            }
            // HANDLER/MERGE/FETCH related END

            Integer limit = querySpec.limit();
            if (limit != null && limit + querySpec.offset() > Constants.PAGE_SIZE) {
                collectNode.downstreamNodes(Collections.singletonList(context.consumerContext().plannerContext().clusterService().localNode().id()));
                collectNode.downstreamExecutionNodeId(localMergeNode.executionNodeId());
            }
            context.result(true);
            QueryThenFetch qtf = new QueryThenFetch(collectNode, localMergeNode, qtfContext);
            return qtf;
        }

        @Override
        protected AnalyzedRelation visitAnalyzedRelation(AnalyzedRelation relation, Context context) {
            return relation;
        }

        @Override
        public AnalyzedRelation visitMultiSourceSelect(MultiSourceSelect multiSourceSelect, Context context) {
            try {
                CrossJoinConsumer.planInnerRelations(multiSourceSelect, context, this, analysisMetaData);
            } catch (ValidationException e) {
                context.consumerContext().validationException(e);
            }
            return multiSourceSelect;
        }
    }

    static class OutputOrderReferenceContext {

        private List<Reference> outputReferences = new ArrayList<>();
        private List<Reference> orderByReferences = new ArrayList<>();
        public boolean collectOrderBy = false;

        public void addReference(Reference reference) {
            if (collectOrderBy) {
                orderByReferences.add(reference);
            } else {
                outputReferences.add(reference);
            }
        }

        public boolean outputsAreAllOrdered() {
            return orderByReferences.containsAll(outputReferences);
        }

    }

    static class OutputOrderReferenceCollector extends SymbolVisitor<OutputOrderReferenceContext, Void> {

        public OutputOrderReferenceContext collect(List<Symbol> symbols) {
            OutputOrderReferenceContext context = new OutputOrderReferenceContext();
            collect(symbols, context);
            return context;
        }

        public void collect(List<Symbol> symbols, OutputOrderReferenceContext context) {
            for (Symbol symbol : symbols) {
                process(symbol, context);
            }
        }

        @Override
        public Void visitAggregation(Aggregation aggregation, OutputOrderReferenceContext context) {
            for (Symbol symbol : aggregation.inputs()) {
                process(symbol, context);
            }
            return null;
        }

        @Override
        public Void visitReference(Reference symbol, OutputOrderReferenceContext context) {
            context.addReference(symbol);
            return null;
        }

        @Override
        public Void visitDynamicReference(DynamicReference symbol, OutputOrderReferenceContext context) {
            return visitReference(symbol, context);
        }

        @Override
        public Void visitFunction(Function function, OutputOrderReferenceContext context) {
            for (Symbol symbol : function.arguments()) {
                process(symbol, context);
            }
            return null;
        }
    }

    static class ReferencesCollectorContext {
        private List<Reference> outputReferences = new ArrayList<>();

        public void addReference(Reference reference) {
            outputReferences.add(reference);
        }

        public boolean containsAnyReference() {
            return !outputReferences.isEmpty();
        }
    }

    static class ReferencesCollector extends SymbolVisitor<ReferencesCollectorContext, Void> {

        public ReferencesCollectorContext collect(List<Symbol> symbols) {
            ReferencesCollectorContext context = new ReferencesCollectorContext();
            collect(symbols, context);
            return context;
        }

        public void collect(List<Symbol> symbols, ReferencesCollectorContext context) {
            for (Symbol symbol : symbols) {
                process(symbol, context);
            }
        }

        @Override
        public Void visitAggregation(Aggregation aggregation, ReferencesCollectorContext context) {
            for (Symbol symbol : aggregation.inputs()) {
                process(symbol, context);
            }
            return null;
        }

        @Override
        public Void visitReference(Reference symbol, ReferencesCollectorContext context) {
            context.addReference(symbol);
            return null;
        }

        @Override
        public Void visitDynamicReference(DynamicReference symbol, ReferencesCollectorContext context) {
            return visitReference(symbol, context);
        }

        @Override
        public Void visitFunction(Function function, ReferencesCollectorContext context) {
            for (Symbol symbol : function.arguments()) {
                process(symbol, context);
            }
            return null;
        }

    }
}
