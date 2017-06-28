package com.yhd.nav.hive.queryparser;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class Table
{
    private String dbName;
    private String tableName;
    private List<Column> allCols;
    private List<Column> projectionDeps;
    private Set<Column> predicateDeps;

    @VisibleForTesting
    public Table(String dbName, String tableName)
    {
        if (dbName != null)
            this.dbName = dbName.toUpperCase();
        this.tableName = tableName.toUpperCase();
    }

    public List<Collection<Column>> getProjectionDeps()
    {
        List<Collection<Column>> returnList = Lists.newArrayList();
        for (Column projectionDep : projectionDeps) {
            List<Column> item = null;
            if (Column.CONST_COL != projectionDep)
                item = projectionDep.getProviderColumns() == null ?
                        Lists.newArrayList(projectionDep) :
                        Lists.newArrayList(projectionDep.getProviderColumns());
            returnList.add(item != null ? item : Collections.<Column>emptyList());
        }
        return returnList;
    }

    public Set<Column> getPredicateDeps()
    {
        return predicateDeps != null ? predicateDeps : Collections.<Column>emptySet();
    }

    public String getName()
    {
        return tableName;
    }

    public String getDbName()
    {
        return dbName;
    }

    String getFQName()
    {
        return dbName + "." + tableName;
    }

    @VisibleForTesting
    public Column addColumn(Column c)
    {
        if (allCols == null)
            allCols = Lists.newArrayList();
        Column existing = getColForName(c.getName());
        if (existing == null)        {
            allCols.add(c);
            return c;
        }
        Preconditions.checkState(c.getProviderColumns() == null);
        return existing;
    }

    Column getColForName(String name)
    {
        for (Column c : allCols)
            if (name.toUpperCase().equals(c.getName()))
                return c;
        return null;
    }

    void addPredicateDep(Column c)
    {
        if (predicateDeps == null)
            predicateDeps = Sets.newHashSet();

        if (c.getProviderColumns() == null)
            predicateDeps.add(c);
        else
            predicateDeps.addAll(c.getProviderColumns());
    }

    void addProjectedDep(Column c)
    {
        if (projectionDeps == null)
            projectionDeps = Lists.newArrayList();
        projectionDeps.add(c);
    }

    public Collection<Column> getAllCols()
    {
        return allCols;
    }

    boolean isSynthetic()
    {
        return Strings.isNullOrEmpty(dbName);
    }

    public int hashCode()
    {
        return getFQName().hashCode();
    }

    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Table other = (Table)obj;
        return getFQName().equals(other.getFQName());
    }

    @VisibleForTesting
    public String toString()
    {
        return getFQName();
    }

    public Collection<Column> getNonPartCols()
    {
        Collection<Column> returnCols = Lists.newArrayListWithCapacity(allCols.size());
        for (Column col : allCols)
            if (!col.isPartColumn())
                returnCols.add(col);
        return returnCols;
    }
}

