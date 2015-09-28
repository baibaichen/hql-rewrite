import com.yhd.hive.HQLConvert;
import com.yhd.hive.queryparser.PrePostOrderTraversor;
import com.yhd.hive.queryparser.TestVisitor;
import org.antlr.runtime.TokenRewriteStream;
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

public class MainTest {
    private static final Logger LOG = LoggerFactory.getLogger(MainTest.class);
    public static void main(String args[]){
        try {
            HiveConf conf = new HiveConf();
            Context ctx = new Context(conf);
            //String query  = "select * from db.tbl";

            String createHQLExternal =  "CREATE EXTERNAL TABLE test.dim_mrchnt(\n" +
                    "  mrchnt_skid bigint, \n" +
                    "  mrchnt_id bigint, \n" +
                    "  mrchnt_name string, \n" +
                    "  mc_site_id bigint, \n" +
                    "  biz_unit bigint, \n" +
                    "  mrchnt_type bigint, \n" +
                    "  mrchnt_co_name string, \n" +
                    "  comsn_rate double, \n" +
                    "  delet_flag int, \n" +
                    "  start_date string, \n" +
                    "  end_date string, \n" +
                    "  ver_num bigint, \n" +
                    "  cur_flag int, \n" +
                    "  etl_batch_id bigint, \n" +
                    "  updt_time string, \n" +
                    "  mrchnt_opat_type int, \n" +
                    "  mrchnt_city_id bigint)";

            String createHQL =  "CREATE TABLE test.dim_mrchnt(\n" +
                    "  mrchnt_skid bigint, \n" +
                    "  mrchnt_id bigint, \n" +
                    "  mrchnt_name string, \n" +
                    "  mc_site_id bigint, \n" +
                    "  biz_unit bigint, \n" +
                    "  mrchnt_type bigint, \n" +
                    "  mrchnt_co_name string, \n" +
                    "  comsn_rate double, \n" +
                    "  delet_flag int, \n" +
                    "  start_date string, \n" +
                    "  end_date string, \n" +
                    "  ver_num bigint, \n" +
                    "  cur_flag int, \n" +
                    "  etl_batch_id bigint, \n" +
                    "  updt_time string, \n" +
                    "  mrchnt_opat_type int, \n" +
                    "  mrchnt_city_id bigint)";

            String dropHQL = "drop table if exists\n" +
                    " xx.trivial;\r\n" +
                    "drop table if exists xx.xxttrr";
            BufferedReader in4 = new BufferedReader(new StringReader(dropHQL));

            HQLConvert hanlder = new HQLConvert(ctx);
            hanlder.processReader(in4);

        }catch (ParseException ex){
            LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(ex));
        }catch (IOException ex){
        }
    }
}
