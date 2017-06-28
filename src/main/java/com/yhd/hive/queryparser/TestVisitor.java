package com.yhd.hive.queryparser;

import com.google.common.base.Preconditions;
import org.antlr.runtime.TokenRewriteStream;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestVisitor implements Visitor {
  private static final Logger LOG = LoggerFactory.getLogger(TestVisitor.class);

  private TokenRewriteStream rewriteStream;

  public TestVisitor(TokenRewriteStream rewriteStream) {
    this.rewriteStream = rewriteStream;
  }

  public void preVisit(Node paramNode) {

  }

  public void visit(Node node) {
    if (!(node instanceof ASTNode)) {
      return;
    }
    ASTNode astNode = (ASTNode) node;
    LOG.debug("Visiting {} at position [{},{}] ", astNode.getText(),
      astNode.getTokenStartIndex(), astNode.getTokenStopIndex());
    switch (astNode.getType()) {
      case HiveParser.TOK_TABNAME:
        processTableName(astNode);
        break;
      default:
        break;
    }
  }

  private void processTableName(ASTNode node) {

    Preconditions.checkState(node.getChildCount() == 2,
      "Database name is missing for table %s",
      node.getChild(0).getText());
    if (isTarget(node)) {
      rewriteStream.replace(node.getTokenStartIndex(), "replaced");
    } else {
      ASTNode DB = (ASTNode) node.getChild(0);
      ASTNode Table = (ASTNode) node.getChild(1);
      LOG.info("{}.{}", DB.getText(),Table.getText());
    }
  }

  private boolean isTarget(ASTNode node) {
    Preconditions.checkState(node.getParent() instanceof ASTNode);
    ASTNode pt = (ASTNode) node.getParent();
    boolean createOrDropTable =
      pt.getType() == HiveParser.TOK_CREATETABLE
        ||
        pt.getType() == HiveParser.TOK_DROPTABLE;
    if (createOrDropTable) {
      return true;
    }
    if (pt.getType() == HiveParser.TOK_TAB) {
      Preconditions.checkState(pt.getParent() instanceof ASTNode);
      pt = (ASTNode) pt.getParent();
      if (pt == null || pt.getType() != HiveParser.TOK_DESTINATION) {
        return false;
      }
      Preconditions.checkState(pt.getParent() instanceof ASTNode);
      pt = (ASTNode) pt.getParent();
      if (pt != null && pt.getType() == HiveParser.TOK_INSERT) {
        return true;
      }
    }
    return false;
  }
}
