package com.yhd.nav.hive.queryparser;

import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;


public class LateralViewVisitor extends AbstractLateralViewVisitor
{
    public void visit(Node node){
        if (!(node instanceof ASTNode))
            return;
        ASTNode astNode = (ASTNode)node;

        //TODO: change const to type
        switch (astNode.getType()){
            case HiveParser.TOK_LATERAL_VIEW:       //699
            case HiveParser.TOK_LATERAL_VIEW_OUTER: //700
                moveLeftMostChildToEnd(node);
                break;
        }
    }
}
