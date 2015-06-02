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

import io.crate.Constants;
import io.crate.analyze.Analyzer;
import io.crate.analyze.ParameterContext;
import io.crate.metadata.ReferenceInfos;
import io.crate.operation.aggregation.impl.AggregationImplModule;
import io.crate.operation.operator.OperatorModule;
import io.crate.operation.predicate.PredicateModule;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.planner.NoopPlan;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.PlannerTest;
import io.crate.planner.node.dql.AbstractDQLPlanNode;
import io.crate.planner.node.dql.MergeNode;
import io.crate.planner.node.dql.QueryThenFetch;
import io.crate.planner.node.dql.join.NestedLoop;
import io.crate.planner.node.dql.join.NestedLoopNode;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.symbol.Symbol;
import io.crate.sql.parser.SqlParser;
import io.crate.testing.TestingHelpers;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

public class CrossJoinConsumerTest {

    private Analyzer analyzer;
    private Planner planner;

    private ThreadPool threadPool;
    private PlannerTest.TestModule testModule;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        threadPool = TestingHelpers.newMockedThreadPool();
        testModule = PlannerTest.plannerTestModule(threadPool);
        Injector injector = new ModulesBuilder()
                .add(testModule)
                .add(new AggregationImplModule())
                .add(new ScalarFunctionModule())
                .add(new PredicateModule())
                .add(new OperatorModule())
                .createInjector();
        analyzer = injector.getInstance(Analyzer.class);
        planner = injector.getInstance(Planner.class);
    }

    private Plan plan(String statement) {
        return planner.plan(analyzer.analyze(SqlParser.createStatement(statement),
                new ParameterContext(new Object[0], new Object[0][], ReferenceInfos.DEFAULT_SCHEMA_NAME)));
    }

    @Test
    public void testWhereWithNoMatchShouldReturnNoopPlan() throws Exception {
        Plan plan = plan("select * from users u1, users u2 where 1 = 2");
        assertThat(plan, instanceOf(NoopPlan.class));
    }

    @Test
    public void testCrossJoinWithoutFetch() throws Exception {
        NestedLoop nestedLoop = (NestedLoop)plan("select users.name, characters.name from users, characters order by users.name, characters.name");

        QueryThenFetch left = (QueryThenFetch)nestedLoop.left();
        QueryThenFetch right = (QueryThenFetch)nestedLoop.right();
        assertThat(left.collectNode().limit(), is(Constants.DEFAULT_SELECT_LIMIT));
        assertThat(right.collectNode().limit(), is(Constants.DEFAULT_SELECT_LIMIT));
        assertThat(left.mergeNode(), is(nullValue()));
        assertThat(right.mergeNode(), is(nullValue()));

        assertThat(left.collectNode().downstreamExecutionNodeId(), is(nestedLoop.leftMergeNode().executionNodeId()));
        assertThat(right.collectNode().downstreamExecutionNodeId(), is(nestedLoop.rightMergeNode().executionNodeId()));

        NestedLoopNode nl = nestedLoop.nestedLoopNode();

        assertThat(nestedLoop.leftMergeNode().downstreamExecutionNodeId(), is(nl.leftExecutionNodeId()));
        assertThat(nestedLoop.leftMergeNode().orderByIndices().length, is(1));
        assertThat(nestedLoop.leftMergeNode().orderByIndices()[0], is(0));
        assertThat(nestedLoop.rightMergeNode().downstreamExecutionNodeId(), is(nl.rightExecutionNodeId()));
        assertThat(nestedLoop.rightMergeNode().orderByIndices().length, is(1));
        assertThat(nestedLoop.rightMergeNode().orderByIndices()[0], is(0));

        assertThat(nl.projections().size(), is(1));
        TopNProjection topNProjection = (TopNProjection)nl.projections().get(0);
        assertThat(topNProjection.limit(), is(Constants.DEFAULT_SELECT_LIMIT));
        assertThat(topNProjection.offset(), is(0));

        MergeNode localMergeNode = nestedLoop.localMergeNode();
        assertThat(nl.downstreamExecutionNodeId(), is(localMergeNode.executionNodeId()));
        assertThat(localMergeNode.projections().size(), is(0));

        assertThat(nl.downstreamExecutionNodeId(), is(localMergeNode.executionNodeId()));
    }

    @Test
    public void testExplicitCrossJoinWith3Tables() throws Exception {
        NestedLoop nl = (NestedLoop)plan("select u1.name, u2.name, u3.name from users u1 cross join users u2 cross join users u3");

        assertThat(nl.resultNode().outputTypes().size(), is(3));
        for (DataType dataType : nl.resultNode().outputTypes()) {
            assertThat(dataType, equalTo((DataType) DataTypes.STRING));
        }
        assertThat(nl.nestedLoopNode().outputTypes().size(), is(3));

        NestedLoop innerNL = (NestedLoop)nl.left();
        QueryThenFetch innerLeftQTF = (QueryThenFetch)innerNL.left();
        QueryThenFetch innerRightQTF = (QueryThenFetch)innerNL.right();
        assertThat(innerLeftQTF.mergeNode(), is(nullValue()));
        assertThat(innerLeftQTF.collectNode().downstreamExecutionNodeId(), is(innerNL.leftMergeNode().executionNodeId()));
        assertThat(innerNL.leftMergeNode().downstreamExecutionNodeId(), is(innerNL.nestedLoopNode().leftExecutionNodeId()));

        assertThat(innerRightQTF.mergeNode(), is(nullValue()));
        assertThat(innerRightQTF.collectNode().downstreamExecutionNodeId(), is(innerNL.rightMergeNode().executionNodeId()));
        assertThat(innerNL.rightMergeNode().downstreamExecutionNodeId(), is(innerNL.nestedLoopNode().rightExecutionNodeId()));

        assertThat(innerNL.localMergeNode(), is(nullValue()));
        /* left PlanNode is a NestedLoopNode, so there is no leftMergeNode.
           The inner NestedLoopNode downstreams directly to the outer NestedLoopNode */
        assertThat(nl.leftMergeNode(), is(nullValue()));
        assertThat(innerNL.nestedLoopNode().downstreamExecutionNodeId(), is(nl.nestedLoopNode().leftExecutionNodeId()));

        assertThat(nl.rightMergeNode().downstreamExecutionNodeId(), is(nl.nestedLoopNode().rightExecutionNodeId()));
    }

    // TODO: test a plan without fetch phase

    @Test
    public void testCrossJoinTwoTablesWithLimit() throws Exception {
        NestedLoop nl = (NestedLoop)plan("select u1.id, u2.id from users u1, users u2 order by u1.id, u2.id limit 2");
        QueryThenFetch left = (QueryThenFetch)nl.left();
        QueryThenFetch right = (QueryThenFetch)nl.right();

        assertThat(left.collectNode().limit(), is(2));
        assertThat(right.collectNode().limit(), is(2));

        assertThat(nl.resultNode().projections().size(), is(0));

        TopNProjection topNProjection = (TopNProjection)nl.nestedLoopNode().projections().get(0);
        assertThat(topNProjection.limit(), is(2));
    }

    /*
    @Test
    public void testCrossJoinWithTwoColumnsAndAddSubtractInResultColumns() throws Exception {
        NestedLoop nl = (NestedLoop)plan("select t1.id, t2.id, t1.id + cast(t2.id as integer) from users t1, characters t2 order by t1.id, t2.id");

        QueryThenFetch left = (QueryThenFetch) nl.left();
        QueryThenFetch right = (QueryThenFetch) nl.right();

        // t1 outputs: [ id ]
        // t2 outputs: [ id, cast(id as int) ]

        TopNProjection topNProjection = (TopNProjection) nl.nestedLoopNode().projections().get(0);
        InputColumn inputCol1 = (InputColumn) topNProjection.outputs().get(0);
        InputColumn inputCol2 = (InputColumn) topNProjection.outputs().get(1);
        Function add = (Function) topNProjection.outputs().get(2);

        assertThat((InputColumn) add.arguments().get(0), equalTo(inputCol1));

        InputColumn inputCol3 = (InputColumn) add.arguments().get(1);

        // topN projection outputs: [ {point to t1.id}, {point to t2.id}, add( {point to t2.id}, {point to cast(t2.id) }]
        List<Symbol> allOutputs = new ArrayList<>(outputs(left.collectNode()));
        allOutputs.addAll(outputs(right.collectNode()));

        Reference ref1 = (Reference) allOutputs.get(inputCol1.index());
        assertThat(ref1.ident().columnIdent().name(), is("id"));
        assertThat(ref1.ident().tableIdent().name(), is("users"));

        Reference ref2 = (Reference) allOutputs.get(inputCol2.index());
        assertThat(ref2.ident().columnIdent().name(), is("id"));
        assertThat(ref2.ident().tableIdent().name(), is("characters"));

        Symbol castFunction = allOutputs.get(inputCol3.index());
        assertThat(castFunction, isFunction("toInt"));
    }*/

    @Test
    public void testCrossJoinsWithSubscript() throws Exception {
        NestedLoop nl = (NestedLoop)plan("select address['street'], ignored_nested.details['no_such_column'] from users cross join ignored_nested");
        assertThat(nl.resultNode().outputTypes().size(), is(2));
        assertThat(nl.resultNode().outputTypes().get(0).id(), is(DataTypes.STRING.id()));
        assertThat(nl.resultNode().outputTypes().get(1).id(), is(DataTypes.UNDEFINED.id()));
    }

    private List<Symbol> outputs(AbstractDQLPlanNode planNode) {
        int projectionIdx = planNode.projections().size() - 1;
        return (List<Symbol>)planNode.projections().get(projectionIdx).outputs();
    }
}