package com.yhd.nav.hive.queryparser;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
//import java.io.PrintStream;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ParserContext
{
    private Set<Table> tgtTbls;
    private List<Column> projectedCols;
    private Set<Column> predicateCols;
    private List<Set<Table>> currentSrcTblsCollection;
    private Table currentTgtTbl;
    private List<Map<String, Table>> currentAliasMapCollection;
    private Map<String, Table> tableNameToTable;
    private final List<State> parsingStates = Lists.newArrayList();
    private final String defaultDbName;

    public ParserContext(String defaultDbName)
    {
        this.defaultDbName = defaultDbName;
        this.currentSrcTblsCollection = Lists.newArrayList();
        this.currentAliasMapCollection = Lists.newArrayList();
    }

    public Set<Table> getTgtTables()
    {
        return tgtTbls != null ? tgtTbls :
            Collections.<Table>emptySet();
    }

    public Set<Table> getSrcTables()
    {
        if (tableNameToTable == null) {
            return Collections.emptySet();
        }
        Set<Table> returnSet = Sets.newHashSet();
        for (Table t : tableNameToTable.values()) {
            if (!t.isSynthetic()) {
                returnSet.add(t);
            }
        }
        return returnSet;
    }

    public List<Column> getProjectedColumns()
    {
        if (projectedCols == null) {
            return Collections.emptyList();
        }
        while (projectedCols.remove(Column.CONST_COL)) {}
        return projectedCols;
    }

    public Set<Column> getPredicateColumns()
    {
        return predicateCols != null ? predicateCols :
            Collections.<Column>emptySet();
    }

    String getDefaultDbName()
    {
        return defaultDbName;
    }

    enum State
    {
        SELECT,  
        FROM,  
        WHERE,  
        INSERT,  
        UNION,  
        ORDER_BY,  
        GROUP_BY,  
        CREATE;

        State() {}
    }

    void setState(State state)
    {
        parsingStates.add(state);
    }

    void clearState()
    {
        parsingStates.remove(parsingStates.size() - 1);
    }

    State getCurrentParsingState()
    {
        return (State)parsingStates.get(parsingStates.size() - 1);
    }

    void addTable(Table t)
    {
        if (getCurrentParsingState() == State.INSERT) {
            addCurrentTgtTable(t);
        } else {
            addSrcTable(t);
        }
    }

    void addCurrentTgtTable(Table t)
    {
        Preconditions.checkState(currentTgtTbl == null);
        currentTgtTbl = t;
    }

    void addSrcTable(Table t)
    {
        if (getCurrentSrcTables() == null)
        {
            Set<Table> cSrcTbls = Sets.newHashSet();
            currentSrcTblsCollection.add(cSrcTbls);
        }
        getCurrentSrcTables().add(t);
        if (tableNameToTable == null) {
            tableNameToTable = Maps.newHashMap();
        }
        tableNameToTable.put(t.getName(), t);
    }

    void addAlias(String aliasName, Table t)
    {
        if (getCurrentAliases() == null)
        {
            Map<String, Table> aToT = Maps.newHashMap();
            currentAliasMapCollection.add(aToT);
        }
        getCurrentAliases().put(aliasName.toUpperCase(), t);
    }

    Table getTableForName(String tableName){
        String tabName = tableName.toUpperCase();
        Table aliasedTable = null;
        for (Map<String, Table> aToT : currentAliasMapCollection)
            aliasedTable = aToT.get(tabName);

        return aliasedTable != null ? aliasedTable:
                (tableNameToTable != null ? tableNameToTable.get(tabName):null);
    }

    void addPredicateColumn(Column c)
    {
        if ((getCurrentParsingState() == State.FROM) || (getCurrentParsingState() == State.WHERE)) {
            if (currentTgtTbl == null)
            {
                if (predicateCols == null) {
                    predicateCols = Sets.newHashSet();
                }
                predicateCols.add(c);
            }
            else
            {
                Preconditions.checkState((projectedCols == null) || (projectedCols.contains(c)));

                currentTgtTbl.addPredicateDep(c);
            }
        }
    }

    void addProjectedColumn(Column c)
    {
        if ((getCurrentParsingState() == State.SELECT) || (getCurrentParsingState() == State.UNION)) {
            if (currentTgtTbl == null)
            {
                if (projectedCols == null) {
                    projectedCols = Lists.newArrayList();
                }
                projectedCols.add(c);
            }
            else
            {
                Preconditions.checkState((projectedCols == null) || (projectedCols.contains(c)));

                currentTgtTbl.addProjectedDep(c);
            }
        }
    }

    Set<Table> getCurrentSrcTables()
    {
        return (Set)Iterables.getLast(currentSrcTblsCollection, null);
    }

    Map<String, Table> getCurrentAliases()
    {
        return (Map)Iterables.getLast(currentAliasMapCollection, null);
    }

    void setCompleted(boolean clearProjectedCols)
    {
        if (!currentSrcTblsCollection.isEmpty()) {
            currentSrcTblsCollection.remove(currentSrcTblsCollection.size() - 1);
        }
        if (!currentAliasMapCollection.isEmpty()) {
            currentAliasMapCollection.remove(currentAliasMapCollection.size() - 1);
        }
        if (clearProjectedCols) {
            projectedCols = null;
        }
    }

    void clearProjectedColumns()
    {
        projectedCols = null;
    }

    void moveTgtTables()
    {
        if (tgtTbls == null) {
            tgtTbls = Sets.newHashSet();
        }
        if (currentTgtTbl != null)
        {
            tgtTbls.add(currentTgtTbl);
            if (predicateCols != null) {
                for (Column col : predicateCols) {
                    currentTgtTbl.addPredicateDep(col);
                }
            }
        }
        currentTgtTbl = null;
    }

    //@VisibleForTesting
    public void print()
    {
        if (tgtTbls != null)
        {
            System.out.println("\n*** Tgt Tables ***");
            for (Table t : tgtTbls) {
                System.out.println(t.getFQName());
            }
        }
        if (projectedCols != null)
        {
            System.out.println("\n*** Projected Cols ***");
            for (Column projectedCol : projectedCols)
            {
                System.out.print(projectedCol.getFQName());
                Collection<Column> providerCols = projectedCol.getProviderColumns();
                if (providerCols != null)
                {
                    System.out.print("----->");
                    for (Column pc : providerCols) {
                        System.out.print(pc.getFQName() + ",");
                    }
                }
                System.out.println("");
            }
        }
        if (predicateCols != null)
        {
            System.out.println("\n*** Conditional Cols ***");
            for (Column conditionalCol : predicateCols) {
                System.out.println(conditionalCol.getFQName() + ",");
            }
        }
    }

    public void createSubContext()
    {
        Set<Table> cSrcTbls = Sets.newHashSet();
        currentSrcTblsCollection.add(cSrcTbls);
        Map<String, Table> aToT = Maps.newHashMap();
        currentAliasMapCollection.add(aToT);
    }
}
