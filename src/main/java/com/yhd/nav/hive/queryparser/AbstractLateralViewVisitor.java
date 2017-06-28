package com.yhd.nav.hive.queryparser;

import com.google.common.base.Preconditions;
import java.util.List;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;


abstract class AbstractLateralViewVisitor implements Visitor {

    public void preVisit(Node node) {}

    protected void moveLeftMostChildToEnd(Node node)
    {
        List<? extends Node> children = node.getChildren();
        Preconditions.checkState(children.size() >= 2);
        ASTNode leftMostChild = (ASTNode)children.get(0);
        ASTNode aNode = (ASTNode)node;
        aNode.deleteChild(0);
        aNode.addChild(leftMostChild);
    }
}

