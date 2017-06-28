package com.yhd.nav.hive.queryparser;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hive.ql.lib.Node;

class PrePostOrderTraversor
{
  private final Visitor visitor;
  
  public PrePostOrderTraversor(Visitor visitor)
  {
    this.visitor = visitor;
  }
  
  void traverse(Node node)
  {
    Preconditions.checkNotNull(node);
    this.visitor.preVisit(node);
    if (node.getChildren() != null) {
      for (Node childNode : node.getChildren()) {
        traverse(childNode);
      }
    }
    this.visitor.visit(node);
  }
}
