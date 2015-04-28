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

import io.crate.action.job.ContextPreparer;
import io.crate.analyze.WhereClause;
import io.crate.breaker.RamAccountingContext;
import io.crate.integrationtests.SQLTransportIntegrationTest;
import io.crate.jobs.JobContextService;
import io.crate.jobs.JobExecutionContext;
import io.crate.metadata.*;
import io.crate.operation.operator.EqOperator;
import io.crate.operation.scalar.FormatFunction;
import io.crate.planner.Planner;
import io.crate.planner.node.ExecutionNode;
import io.crate.planner.node.dml.DeleteByQueryNode;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Symbol;
import io.crate.test.integration.CrateIntegrationTest;
import io.crate.testing.TestingHelpers;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.action.support.PlainActionFuture;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isOneOf;
import static org.mockito.Mockito.mock;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.GLOBAL)
public class InternalDeleteOperationTest extends SQLTransportIntegrationTest{

    @Test
    public void testDelete() throws Exception {
        execute("create table t (name string) with (number_of_replicas = 0)");
        ensureYellow();
        execute("insert into t (name) values ('Marvin'), ('Arthur'), ('Trillian')");
        execute("refresh table t");

        // WHERE name = format('M%s', 'arvin')
        WhereClause whereClause = TestingHelpers.whereClause(
                EqOperator.NAME,
                TestingHelpers.createReference("name", DataTypes.STRING),
                    new Function(
                        new FunctionInfo(new FunctionIdent(FormatFunction.NAME, Arrays.<DataType>asList(DataTypes.STRING, DataTypes.STRING)), DataTypes.STRING),
                            Arrays.<Symbol>asList(Literal.newLiteral("M%s"), Literal.newLiteral("arvin"))
                    )
                );

        Routing routing = cluster().getInstance(ReferenceInfos.class).getTableInfo(new TableIdent(null, "t")).getRouting(whereClause, null);
        Planner.Context plannerContext = new Planner.Context(clusterService());
        plannerContext.allocateJobSearchContextIds(routing);

        DeleteByQueryNode deleteByQueryNode = new DeleteByQueryNode(0, routing, whereClause, null);
        deleteByQueryNode.jobId(UUID.randomUUID());

        createJobContext(deleteByQueryNode, deleteByQueryNode.jobId().get());

        for (DeleteOperation deleteOperation : cluster().getInstances(DeleteOperation.class)) {
            PlainActionFuture<Long> future = new PlainActionFuture<>();
            deleteOperation.delete(deleteByQueryNode, mock(RamAccountingContext.class), future);
            Long result = future.actionGet();
            assertThat(result, isOneOf(0L, 1L));
        }
        execute("refresh table t");
        execute("select * from t order by name");
        assertThat(TestingHelpers.printedTable(response.rows()), is("Arthur\nTrillian\n"));
    }

    private List<JobExecutionContext> createJobContext(ExecutionNode deleteByQueryNode, UUID jobId) {
        ContextPreparer contextPreparer = cluster().getInstance(ContextPreparer.class);

        List<JobExecutionContext> executionContexts = new ArrayList<>(2);
        for (JobContextService jobContextService : cluster().getInstances(JobContextService.class)) {
            JobExecutionContext.Builder builder = jobContextService.newBuilder(jobId);
            contextPreparer.prepare(jobId, deleteByQueryNode, builder);
            executionContexts.add(jobContextService.createOrMergeContext(builder));
        }
        return executionContexts;
    }
}