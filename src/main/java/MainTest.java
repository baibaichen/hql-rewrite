import com.yhd.hive.queryparser.PrePostOrderTraversor;
import com.yhd.hive.queryparser.TestVisitor;
import org.antlr.runtime.TokenRewriteStream;
import org.apache.commons.logging.Log;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

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

            String dropHQL = "drop table if exists xx.trivial\n" +
                    "drop table if exists xx.xxttrr";
            ParseDriver pd = new ParseDriver();
            ASTNode tree = pd.parse(dropHQL,ctx);
            tree = ParseUtils.findRootNonNullToken(tree);
            TokenRewriteStream ts = ctx.getTokenRewriteStream();

            PrePostOrderTraversor traversor = new PrePostOrderTraversor(new TestVisitor(ts));
            traversor.traverse(tree);


            System.out.println("\nRewritten Query:\n" + ts.toString());
        }catch (ParseException ex){
            LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(ex));
        }catch (IOException ex){
        }
    }
}
