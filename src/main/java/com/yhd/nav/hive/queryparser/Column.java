package com.yhd.nav.hive.queryparser;



import com.google.common.annotations.VisibleForTesting;
import java.util.Set;

public class Column
{
    static final Column CONST_COL = new Column(null, "CONST");
    private Table table;
    private final String name;
    private final Set<Column> providerCols;
    private final Set<Table> providerTables;
    private final boolean isGeneratedColumn;
    private final boolean isPartCol;

    Column(Table table, String name, Set<Column> providerCols)
    {
        this(table, name, null, providerCols, false, false);
    }

    Column(Table table, String name, Set<Table> providerTables, Set<Column> providerColumns, boolean isGeneratedColumn, boolean isPartCol)
    {
        this.table = table;
        this.name = name.toUpperCase();
        this.providerCols = providerColumns;
        this.providerTables = providerTables;
        this.isGeneratedColumn = isGeneratedColumn;
        this.isPartCol = isPartCol;
    }

    @VisibleForTesting
    public Column(Table table, String name)
    {
        this(table, name, false);
    }

    Column(Table table, String name, boolean isPartCol)
    {
        this(table, name, null, null, false, isPartCol);
    }

    public Table getTable()
    {
        return this.table;
    }

    public String getName()
    {
        return this.name;
    }

    String getFQName()
    {
        return this.table != null ? this.table.getFQName() + "." + this.name : this.name;
    }

    public Set<Column> getProviderColumns()
    {
        return this.providerCols;
    }

    public int hashCode()
    {
        return getFQName().hashCode();
    }

    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        Column other = (Column)obj;
        return getFQName().equals(other.getFQName());
    }

    @VisibleForTesting
    public String toString()
    {
        return getFQName();
    }

    public Set<Table> getProviderTables()
    {
        return this.providerTables;
    }

    public boolean isGeneratedColumn()
    {
        return this.isGeneratedColumn;
    }

    boolean isPartColumn()
    {
        return this.isPartCol;
    }
}

