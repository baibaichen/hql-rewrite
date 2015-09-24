package com.yhd.hive.queryparser;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Chang on 2015/9/24.
 */
public class TestVisitor implements Visitor {
    private static final Logger LOG = LoggerFactory.getLogger(TestVisitor.class);

    public void preVisit(Node paramNode) {

    }

    public void visit(Node node) {
        if (!(node instanceof ASTNode))
            return;
        ASTNode astNode = (ASTNode)node;
        LOG.debug("Visiting {} at position {} ", astNode.getText(), Integer.valueOf(astNode.getTokenStartIndex()));
        switch (astNode.getType()){
            case HiveParser.TOK_TABNAME:
                processTableName(astNode);
                break;
            default:
                break;
        }
    }

    private List<ASTNode> getChildren(ASTNode node)
    {
        //TODO: performance????
        if (node.getChildren() == null)
            return null;

        List<ASTNode> tmp = new ArrayList<ASTNode>();
        for (Node exp : node.getChildren())
            tmp.add((ASTNode)exp);
        return tmp;
    }

    private void processTableName(ASTNode node){
        List<ASTNode> children = getChildren(node);

        Preconditions.checkState((children.size() == 2) || (children.size() == 1));
        String tableName = children.get(children.size() - 1).getText();

        System.out.println(tableName);
    }
}
