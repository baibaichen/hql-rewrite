package com.yhd.nav.hive.queryparser;



import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.ArrayList;
import org.antlr.runtime.Token;
import org.antlr.runtime.tree.Tree;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class LineageVisitor
        implements Visitor
{
    private final ParserContext pCtx;
    private final ASTNode rootNode;
    private final HiveParserDao dao;
    private int runningColNumber = 0;
    private static final Logger LOG = LoggerFactory.getLogger(LineageVisitor.class);
    private static final String COL_PREFIX = "__c__";

    public LineageVisitor(ParserContext pCtx, ASTNode rootNode, HiveParserDao dao)
    {
        this.pCtx = pCtx;
        this.rootNode = rootNode;
        this.dao = dao;
    }

    public void preVisit(Node node)
    {
        if (!(node instanceof ASTNode)) {
            return;
        }
        ASTNode astNode = (ASTNode)node;
        LOG.debug("Previsiting {} at position {} ", astNode.getText(), Integer.valueOf(astNode.getTokenStartIndex()));
        switch (astNode.getType())
        {
            case HiveParser.TOK_FROM: //667
                this.pCtx.setState(ParserContext.State.FROM);
                break;
            case HiveParser.TOK_SELECT:      //776
            case HiveParser.TOK_SELECTDI:    //777
                this.pCtx.setState(ParserContext.State.SELECT);
                break;
            case HiveParser.TOK_WHERE:       //871
                this.pCtx.setState(ParserContext.State.WHERE);
                break;
            case HiveParser.TOK_INSERT:      //692
                this.pCtx.setState(ParserContext.State.INSERT);
                break;
            case HiveParser.TOK_UNION:       //860
                this.pCtx.setState(ParserContext.State.UNION);
                break;
            case HiveParser.TOK_ORDERBY:     //738
                this.pCtx.setState(ParserContext.State.ORDER_BY);
                break;
            case HiveParser.TOK_GROUPBY:     //677
                this.pCtx.setState(ParserContext.State.GROUP_BY);
                break;
            case HiveParser.TOK_CREATETABLE: //626
                this.pCtx.setState(ParserContext.State.CREATE);
                break;
            case HiveParser.TOK_SUBQUERY:
                this.pCtx.createSubContext();
                break;
            default:
                LOG.debug("Unhandled AST node: {}", Integer.valueOf(astNode.getType()));
        }
    }

    public void visit(Node node)
    {
        if (!(node instanceof ASTNode)) {
            return;
        }
        ASTNode astNode = (ASTNode)node;
        LOG.debug("Visiting {} at position {} ", astNode.getText(), Integer.valueOf(astNode.getTokenStartIndex()));
        switch (astNode.getType())
        {
            case HiveParser.TOK_FROM:        //667
            case HiveParser.TOK_GROUPBY:     //677
            case HiveParser.TOK_ORDERBY:     //738
            case HiveParser.TOK_SELECT:      //776
            case HiveParser.TOK_SELECTDI:    //777
            case HiveParser.TOK_WHERE:       //871
                this.pCtx.clearState();
                break;
            case HiveParser.TOK_TABNAME:
                processTableName(astNode);
                break;
            case HiveParser.TOK_TABREF :
                processTableRef(astNode);
                break;
            case HiveParser.TOK_TABLE_OR_COL:
                processTableOrColumn(astNode);
                break;
            case HiveParser.DOT:             //17
                processDot(astNode);
                break;
            case HiveParser.TOK_SELEXPR:     //778
                processSelectExpr(astNode);
                break;
            case HiveParser.TOK_SUBQUERY:
                processSubQuery(astNode);
                break;
            case HiveParser.TOK_INSERT:      //692
                processInsert(astNode);
                this.pCtx.clearState();
                break;
            case HiveParser.TOK_QUERY:
                processQuery(astNode);
                break;
            case HiveParser.TOK_UNION:       //860
                processUnion(astNode);
                this.pCtx.clearState();
                break;
            case HiveParser.TOK_ALLCOLREF:   //576
                processAllColsRef(astNode);
                break;
            case HiveParser.TOK_CREATETABLE: //626
                processCreateTable(astNode);
                this.pCtx.clearState();
                break;
            case HiveParser.TOK_TABALIAS:    //819
                processTableGeneratingFunction(astNode);
                break;
            default:
                processDefault(astNode);
        }
    }

    private void processTableGeneratingFunction(ASTNode node)
    {
        List<ASTNode> children = getChildren(node);
        Preconditions.checkState(children.size() == 1);
        ASTNode cNode = (ASTNode)children.get(0);
        Table table = new Table(null, cNode.getText());
        RNode<Table> rNode = new RNode(node, table);
        replaceNode(node, rNode);
    }

    private void processCreateTable(ASTNode node)
    {
        List<ASTNode> children = getChildren(node);
        Preconditions.checkState(children.size() >= 3);
        Preconditions.checkState(children.get(0) instanceof RNode);
        Table t = (Table)((RNode)children.get(0)).getWrappedResultAsType(Table.class);
        Preconditions.checkState(Iterables.getLast(children) instanceof RNode);

        List<Column> cols = (List)((RNode)Iterables.getLast(children)).getWrappedResultAsType(List.class);
        for (Column c : cols)
        {
            Column col = new Column(t, c.getName());
            t.addColumn(col);
            t.addProjectedDep(c);
        }
        this.pCtx.setCompleted(true);
        this.pCtx.addCurrentTgtTable(t);
        this.pCtx.moveTgtTables();
    }

    private void processTableName(ASTNode node)
    {
        List<ASTNode> children = getChildren(node);

        Preconditions.checkState((children.size() == 2) || (children.size() == 1));
        String tableName = ((ASTNode)children.get(children.size() - 1)).getText();
        String dbName = children.size() == 2 ? ((ASTNode)children.get(0)).getText() : this.pCtx.getDefaultDbName();
        //Table t;
        Table t;
        if (this.pCtx.getCurrentParsingState() == ParserContext.State.CREATE)
        {
            t = new Table(dbName, tableName);
        }
        else
        {
            t = this.dao.getTable(dbName, tableName);
            if (t != null) {
                this.pCtx.addTable(t);
            } else {
                t = this.pCtx.getTableForName(tableName);
            }
        }
        RNode<Table> rNode = new RNode(node, t);
        replaceNode(node, rNode);
    }

    private void processTableRef(ASTNode node)
    {
        List<ASTNode> children = getChildren(node);
        if (children.size() == 1) {
            return;
        }
        RNode<Table> tbl = (RNode)children.get(0);
        Table t = tbl.getWrappedResult();
        String aliasName = children.get(children.size() - 1).getText();
        this.pCtx.addAlias(aliasName, t);
    }

    private void processTableOrColumn(ASTNode node)
    {
        if ((this.pCtx.getCurrentParsingState() == ParserContext.State.GROUP_BY) || (this.pCtx.getCurrentParsingState() == ParserContext.State.ORDER_BY)) {
            return;
        }
        List<ASTNode> children = getChildren(node);
        Preconditions.checkState(children.size() == 1);
        String tableOrColName = ((ASTNode)children.get(0)).getText();
        Table t = this.pCtx.getTableForName(tableOrColName);
        if (t != null)
        {
            RNode<Table> rNode = new RNode(node, t);
            replaceNode(node, rNode);
        }
        else
        {
            Collection<Table> tbls = this.pCtx.getCurrentSrcTables();
            for (Table tbl : tbls)
            {
                Column ec = tbl.getColForName(tableOrColName);
                if (ec != null)
                {
                    this.pCtx.addPredicateColumn(ec);
                    RNode<Column> rNode = new RNode(node, ec);
                    replaceNode(node, rNode);
                    return;
                }
            }
            throw new IllegalStateException("Column " + tableOrColName + " not found");
        }
    }

    private void processDot(ASTNode node)
    {
        List<ASTNode> children = getChildren(node);
        if (children.size() != 2) {
            return;
        }
        ASTNode c1Node = (ASTNode)children.get(0);
        if ((c1Node instanceof RNode))
        {
            Table t = (Table)((RNode)c1Node).getWrappedResultAsType(Table.class);
            Column ec = null;
            if (t != null)
            {
                ASTNode c2Node = (ASTNode)children.get(1);
                Column c = new Column(t, c2Node.getText());
                ec = t.addColumn(c);
            }
            else
            {
                Collection<Column> cols = getColumns((RNode)c1Node);
                ec = (Column)Iterables.getOnlyElement(cols);
            }
            if (ec != null)
            {
                this.pCtx.addPredicateColumn(ec);
                RNode<Column> rNode = new RNode(node, ec);
                replaceNode(node, rNode);
            }
        }
    }

    private void processSelectExpr(ASTNode node)
    {
        List<ASTNode> children = getChildren(node);
        ASTNode cNode = (ASTNode)children.get(0);
        if (!(cNode instanceof RNode))
        {
            this.pCtx.addProjectedColumn(Column.CONST_COL);
            return;
        }
        RNode<?> rNode = (RNode)cNode;
        boolean isGeneratedColumn = false;
        Set<Table> providerTables = null;

        Table generatedTable = null;

        Set<String> aliasNames = Sets.newHashSet();
        if (children.size() == 1)
        {
            Collection<Column> cols = getColumns(rNode);
            if (cols.size() == 1)
            {
                this.pCtx.addProjectedColumn((Column)Iterables.getOnlyElement(cols));
                replaceNode(node, rNode);
                return;
            }
            if ((children.size() == 1) && (((ASTNode)children.get(0)).getToken().getType() == HiveParser.TOK_ALLCOLREF))
            {
                for (Column c : cols) {
                    this.pCtx.addProjectedColumn(c);
                }
                replaceNode(cNode, rNode);
                return;
            }
            aliasNames.add("__c__" + this.runningColNumber);
            this.runningColNumber += 1;
            isGeneratedColumn = true;
        }
        else
        {
            Preconditions.checkState(children.size() >= 2);
            ASTNode lastChildNode = (ASTNode)Iterables.getLast(children);
            if ((lastChildNode instanceof RNode)) {
                generatedTable = (Table)((RNode)lastChildNode).getWrappedResultAsType(Table.class);
            }
            int lastColIndex = children.size();
            if (generatedTable != null)
            {
                this.pCtx.addTable(generatedTable);
                lastColIndex--;
            }
            for (int i = 1; i < lastColIndex; i++) {
                aliasNames.add(((ASTNode)children.get(i)).getText());
            }
        }
        if (((ASTNode)children.get(0)).getToken().getType() == HiveParser.TOK_FUNCTIONSTAR/*671*/)
        {
            providerTables = Sets.newHashSet();
            for (Table t : this.pCtx.getCurrentSrcTables()) {
                if (t.getDbName() != null) {
                    providerTables.add(t);
                }
            }
        }
        Set<Column> providerCols = null;
        Collection<Column> level1ProviderCols = getColumns(rNode);
        for (Column pc : level1ProviderCols)
        {
            if (providerCols == null) {
                providerCols = Sets.newHashSet();
            }
            if (pc.getProviderColumns() != null) {
                providerCols.addAll(pc.getProviderColumns());
            } else {
                providerCols.add(pc);
            }
        }
        Collection<Column> resultCols = Lists.newArrayListWithCapacity(aliasNames.size());
        for (String aliasName : aliasNames)
        {
            Column newCol = new Column(generatedTable, aliasName, providerTables, providerCols, isGeneratedColumn, false);
            if (generatedTable != null) {
                generatedTable.addColumn(newCol);
            } else {
                this.pCtx.addProjectedColumn(newCol);
            }
        }
        RNode<Collection<Column>> newNode = new RNode(node, resultCols);

        replaceNode(node, newNode);
    }

    private void processSubQuery(ASTNode node)
    {
        List<ASTNode> children = getChildren(node);
        Preconditions.checkState(children.size() == 2);
        String newTableName = ((ASTNode)children.get(1)).getText();
        Table newTable = new Table("", newTableName);
        Preconditions.checkState(children.get(0) instanceof RNode);
        Collection<Column> projectedCols = getColumns((RNode)children.get(0));
        for (Column col : projectedCols)
        {
            Set<Column> providerCols = col.getProviderColumns() != null ? col.getProviderColumns() : Sets.newHashSet(new Column[] { col });

            Column newCol = new Column(newTable, col.getName(), providerCols);
            newTable.addColumn(newCol);
        }
        Collection<Column> predicateCols = this.pCtx.getPredicateColumns();
        if (predicateCols != null) {
            for (Column c : predicateCols) {
                newTable.addPredicateDep(c);
            }
        }
        this.pCtx.clearProjectedColumns();
        this.pCtx.addTable(newTable);
        this.pCtx.addAlias(newTableName, newTable);
    }

    private void processInsert(ASTNode node)
    {
        this.pCtx.moveTgtTables();
    }

    private void processQuery(ASTNode node)
    {
        List<Column> projectedCols = Lists.newArrayList(this.pCtx.getProjectedColumns());
        RNode<List<Column>> newNode = new RNode(node, projectedCols);
        replaceNode(node, newNode);
        this.pCtx.setCompleted(!node.equals(this.rootNode));
    }

    private void processUnion(ASTNode node)
    {
        List<ASTNode> children = getChildren(node);
        Preconditions.checkState(children.size() == 2);

        List<Column> child1 = (List)((RNode)children.get(0)).getWrappedResultAsType(List.class);

        List<Column> child2 = (List)((RNode)children.get(1)).getWrappedResultAsType(List.class);

        Preconditions.checkState(child1.size() == child2.size());
        int i = 0;
        for (Column c1 : child1)
        {
            Set<Column> allProviderCols = Sets.newHashSet();
            allProviderCols.addAll(c1.getProviderColumns() != null ? c1.getProviderColumns() : Lists.newArrayList(new Column[] { c1 }));

            Column c2 = (Column)child2.get(i++);
            allProviderCols.addAll(c2.getProviderColumns() != null ? c2.getProviderColumns() : Lists.newArrayList(new Column[] { c2 }));

            Column newCol = new Column(null, c1.getName(), allProviderCols);
            this.pCtx.addProjectedColumn(newCol);
        }
        processQuery(node);
    }

    private void processDefault(ASTNode node)
    {
        List<ASTNode> children = getChildren(node);
        if ((children == null) || (children.isEmpty())) {
            return;
        }
        List<Column> cols = Lists.newArrayList();
        for (ASTNode child : children) {
            if ((child instanceof RNode))
            {
                Collection<Column> resultCols = getColumns((RNode)child);
                if (resultCols != null) {
                    cols.addAll(resultCols);
                }
            }
        }
        RNode<Collection<Column>> newNode = new RNode(node, cols);
        replaceNode(node, newNode);
    }

    private void processAllColsRef(ASTNode node)
    {
        List<ASTNode> children = getChildren(node);
        List<Column> cols = Lists.newArrayList();
        if (children == null)
        {
            for (Table t : this.pCtx.getCurrentSrcTables()) {
                cols.addAll(t.getAllCols());
            }
        }
        else
        {
            ASTNode tabNode = (ASTNode)Iterables.getOnlyElement(children);
            Preconditions.checkState(tabNode instanceof RNode);
            Table t = (Table)((RNode)tabNode).getWrappedResultAsType(Table.class);
            Preconditions.checkState(t != null);
            cols.addAll(t.getAllCols());
        }
        RNode<Collection<Column>> newNode = new RNode(node, cols);

        replaceNode(node, newNode);
    }

    private Collection<Column> getColumns(RNode<?> node)
    {
        Collection<Column> cols = Lists.newArrayList();
        Column c = (Column)node.getWrappedResultAsType(Column.class);
        if (c != null)
        {
            cols.add(c);
        }
        else
        {
            Collection<Column> cs = (Collection)node.getWrappedResultAsType(Collection.class);
            if (cs != null) {
                cols.addAll(cs);
            }
        }
        return cols;
    }
    private List<ASTNode> getChildren(ASTNode node) {
        return (List<ASTNode>)(List<?>) node.getChildren();
    }
    private void replaceNode(ASTNode node, RNode<?> rNode)
    {
        Tree pNode = node.getParent();
        int nodeIndex = node.getChildIndex();
        if (pNode != null) {
            pNode.setChild(nodeIndex, rNode);
        }
    }
}
