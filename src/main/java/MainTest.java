/**
 * Created by ChangDev on 9/24/2015.
 */
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.ParseUtils;

import java.io.IOException;

public class MainTest {
    public static void main(String args[])
    {
        try {
            HiveConf conf = new HiveConf();
            Context ctx = new Context(conf);
            String query = "";
            System.out.println("Hello World");
            ParseDriver pd = new ParseDriver();
            ASTNode tree = pd.parse(query,ctx);
            tree = ParseUtils.findRootNonNullToken(tree);
        }catch (ParseException ex){
        }catch (IOException ex){
        }
    }
}
