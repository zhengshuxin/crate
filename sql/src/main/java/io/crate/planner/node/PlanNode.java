package io.crate.planner.node;


import io.crate.types.DataType;

import java.util.List;
import java.util.Set;

public interface PlanNode {

    <C, R> R accept(PlanNodeVisitor<C, R> visitor, C context);

    List<DataType> outputTypes();
    Set<String> executionNodes();

    //int executionNodeId();
}
