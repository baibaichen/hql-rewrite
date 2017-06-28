package com.yhd.nav.hive.queryparser;


import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryParser {
    private static final Logger LOG = LoggerFactory.getLogger(QueryParser.class);

    public ParserContext parse(String query, String defaultDbName, HiveParserDao parserDao)
            throws ParseException
    {
        LOG.debug("Parsing Query {}", query);
        ParserContext pCtx = new ParserContext(defaultDbName);
        ParseDriver pd = new ParseDriver();
        ASTNode tree = pd.parse(query);
        tree = ParseUtils.findRootNonNullToken(tree);

        modifyTreeForHandlingLateralViews(tree);
        traverse(pCtx, tree, parserDao);
        return pCtx;
    }

    private void modifyTreeForHandlingLateralViews(ASTNode tree)
    {
        Visitor v = new LateralViewVisitor();
        PrePostOrderTraversor traversor = new PrePostOrderTraversor(v);
        traversor.traverse(tree);
    }

    private void traverse(ParserContext pCtx, ASTNode newTree, HiveParserDao dao)
    {
        LineageVisitor lv = new LineageVisitor(pCtx, newTree, dao);
        PrePostOrderTraversor traversor = new PrePostOrderTraversor(lv);
        traversor.traverse(newTree);
    }
}
