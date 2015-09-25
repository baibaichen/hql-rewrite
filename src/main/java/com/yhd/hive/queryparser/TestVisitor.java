package com.yhd.hive.queryparser;

import com.google.common.base.Preconditions;
import org.antlr.runtime.ClassicToken;
import org.antlr.runtime.TokenRewriteStream;
import org.antlr.runtime.tree.Tree;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class TestVisitor implements Visitor {
    private static final Logger LOG = LoggerFactory.getLogger(TestVisitor.class);

    private TokenRewriteStream rewriteStream;

    public TestVisitor(TokenRewriteStream rewriteStream) {
        this.rewriteStream = rewriteStream;
    }

    public void preVisit(Node paramNode) {

    }

    public void visit(Node node) {
        if (!(node instanceof ASTNode))
            return;
        ASTNode astNode = (ASTNode)node;
        LOG.debug("Visiting {} at position [{},{}] ", astNode.getText(),
                  astNode.getTokenStartIndex(), astNode.getTokenStopIndex());
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
        Preconditions.checkState(node.getParent() instanceof ASTNode);
        ASTNode pt = (ASTNode)node.getParent();
        if ( pt.getType() == HiveParser.TOK_CREATETABLE
                             ||
            pt.getType() == HiveParser.TOK_DROPTABLE ){

            //find target table
            Preconditions.checkState(node.getChildCount() ==2,
                                     "Database name is missing for table %s",
                                     node.getChild(0).getText());
            rewriteStream.replace(node.getTokenStartIndex(),"replaced");
        }
    }
}
