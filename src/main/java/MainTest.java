import com.yhd.hive.HQLConvert;
import com.yhd.hive.queryparser.PrePostOrderTraversor;
import com.yhd.hive.queryparser.TestVisitor;
import org.antlr.runtime.TokenRewriteStream;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;

import java.util.Map;
import java.util.HashMap;

public class MainTest {
    private static final Logger LOG = LoggerFactory.getLogger(MainTest.class);
    public static void main(String args[]){
        try {

            HashMap<String,String> params = new HashMap<String, String>();
            params.put(      "DB_TMP" , "testtmp");
            params.put(       "DB_DW" , "testdw");
            params.put(      "DB_ODS" , "testods");
            params.put(  "DB_DEFAULT" , "testdefault");
            params.put(      "DB_RPT" , "testrpt");
            params.put(      "DB_BIC" , "testbic");
            params.put(  "DB_DM_MOBL" , "testdm_mobl");
            params.put("DB_WORKSPACE" , "testworkspace");
            params.put("DB_INTERFACE" , "testinterface");
            params.put(       "label" , "$begin");

            HiveConf conf = new HiveConf();
            Context ctx = new Context(conf);
            //String query  = "select * from db.tbl";


            BufferedReader in4 = new BufferedReader(new StringReader(TestConstant.SQL_rpt_trfc_cms));

            HQLConvert hanlder = new HQLConvert(ctx,params);
            hanlder.processReader(in4);

        }catch (ParseException ex){
            LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(ex));
        }catch (IOException ex){
        }
    }
}
