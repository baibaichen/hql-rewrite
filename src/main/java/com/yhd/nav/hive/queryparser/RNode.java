package com.yhd.nav.hive.queryparser;

import org.apache.hadoop.hive.ql.parse.ASTNode;

class RNode<T>
        extends ASTNode
{
    T wrappedResult;

    public RNode(ASTNode n, T o)
    {
        super(n);
        this.wrappedResult = o;
    }

    T getWrappedResult()
    {
        return (T)this.wrappedResult;
    }

    <K> K getWrappedResultAsType(Class<K> klass)
    {
        if (klass.isAssignableFrom(this.wrappedResult.getClass())) {
            return (K)this.wrappedResult;
        }
        return null;
    }
}
