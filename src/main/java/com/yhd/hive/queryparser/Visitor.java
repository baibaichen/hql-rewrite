package com.yhd.hive.queryparser;

import org.apache.hadoop.hive.ql.lib.Node;

public interface Visitor {
    void preVisit(Node paramNode);
    void visit(Node paramNode);
}
