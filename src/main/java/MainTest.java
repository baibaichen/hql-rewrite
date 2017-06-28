import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.yhd.hive.HQLConvert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

public class MainTest {
  private static final Logger LOG = LoggerFactory.getLogger(MainTest.class);
  private static final String HDFS_SESSION_PATH_KEY = "_hive.hdfs.session.path";
  private static final String LOCAL_SESSION_PATH_KEY = "_hive.local.session.path";

  private static BufferedReader loadFileFromResource(String fileName) throws UnsupportedEncodingException, FileNotFoundException {
    //Get file from resources folder
    InputStreamReader isr = new InputStreamReader(
      new FileInputStream(MainTest.class.getResource(fileName).getFile()), "UTF8");
    return  new BufferedReader(isr);
  }

  private static HashMap<String, String> buildOps(){
    HashMap<String, String> params = new HashMap<String, String>();
    params.put("ORA_TMP", "testtmp");
    params.put("ORA_ODS", "o_ods");
    params.put("ORA_DW", "o_dw");
    return  params;
  }
  public static void main(String args[]) {
    try {
      HashMap<String, String> params = new HashMap<String, String>();
      params.put("DB_TMP", "testtmp");
      params.put("DB_DW", "testdw");
      params.put("DB_ODS", "testods");
      params.put("DB_DEFAULT", "testdefault");
      params.put("DB_RPT", "testrpt");
      params.put("DB_BIC", "testbic");
      params.put("DB_DM_MOBL", "testdm_mobl");
      params.put("DB_WORKSPACE", "testworkspace");
      params.put("DB_INTERFACE", "testinterface");
      params.put("label", "$begin");


      Configuration fixedconf = new Configuration();
      fixedconf.set(HDFS_SESSION_PATH_KEY, "/tmp");
      fixedconf.set(LOCAL_SESSION_PATH_KEY, "/tmp");

      HiveConf conf = new HiveConf();
      conf.addResource(fixedconf);
      Context ctx = new Context(conf);

      URL url = Resources.getResource("fin_bkgrnd_gp_detl_utf8.sql");
      String Text =  Resources.toString(url, Charsets.UTF_8);

      //BufferedReader in4 = new BufferedReader(new StringReader(TestConstant.SQL_rpt_trfc_cms));
      //BufferedReader in4 = loadFileFromResource("fin_bkgrnd_gp_detl_utf8.sql");

      BufferedReader in4 = new BufferedReader(new StringReader(Text));
      HQLConvert hanlder = new HQLConvert(ctx, buildOps());
      hanlder.processReader(in4);

    } catch (ParseException ex) {
      LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(ex));
    } catch (FileNotFoundException fex){
      LOG.error("file not found");
    } catch (IOException ex) {
      LOG.error("IO");
    }
  }
}
