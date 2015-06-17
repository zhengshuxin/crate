package io.crate.planner.node.ddl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.crate.planner.node.PlanNode;
import io.crate.types.DataType;
import io.crate.types.LongType;

import java.util.List;
import java.util.Set;

public abstract class RowCountPlanNode implements PlanNode {

    // this is for the number of affected rows
    private static final List<DataType> OUTPUT_TYPES = ImmutableList.<DataType>of(LongType.INSTANCE);

    @Override
    public List<DataType> outputTypes() {
        return OUTPUT_TYPES;
    }

    @Override
    public Set<String> executionNodes() {
        return ImmutableSet.of();
    }
}
