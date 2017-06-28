package com.yhd.nav.hive.queryparser;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import java.util.List;

//-import com.yhd.nav.hive.extractor.HiveExtractorHelper;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.conf.HiveConf;

public class HiveParserDao
{
    private static final Logger LOG = LoggerFactory.getLogger(HiveParserDao.class);
    private final HiveMetaStoreClient metastore;
    private final MapConfiguration config;
    private final HiveConf conf;                // workaround cloudera configuration

    public HiveParserDao(HiveMetaStoreClient metastore, MapConfiguration config)
    {
        //TODO :chang...
        this(metastore,config,null);
    }
    public HiveParserDao(HiveMetaStoreClient metastore, MapConfiguration config,HiveConf conf)
    {
        this.metastore = metastore;
        this.config = config;
        this.conf = conf;
    }

    @VisibleForTesting
    public Table getTable(String dbName, String tableName)
    {
        return null;
//        Table returnTbl;
//        try
//        {
//            org.apache.hadoop.hive.metastore.api.Table table = metastore.getTable(dbName, tableName);
//
//            List<FieldSchema> cols = HiveExtractorHelper.getColumns(table, config,conf);
//            returnTbl = new Table(table.getDbName(), table.getTableName());
//            for (FieldSchema col : cols)
//            {
//                Column c = new Column(returnTbl, col.getName());
//                returnTbl.addColumn(c);
//            }
//            List<FieldSchema> partCols = HiveExtractorHelper.getPartitionColumns(table);
//            for (FieldSchema col : partCols)
//            {
//                Column c = new Column(returnTbl, col.getName(), true);
//                returnTbl.addColumn(c);
//            }
//        }
//        catch (NoSuchObjectException e)
//        {
//            LOG.info("Table {}.{} does not exist.", dbName, tableName);
//            return null;
//        }
//        catch (Exception e)
//        {
//            throw Throwables.propagate(e);
//        }
//        return returnTbl;
    }
}