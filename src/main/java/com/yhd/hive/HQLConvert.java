package com.yhd.hive;

import com.yhd.hive.queryparser.PrePostOrderTraversor;
import com.yhd.hive.queryparser.TestVisitor;
import org.antlr.runtime.TokenRewriteStream;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.text.StrSubstitutor;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Map;

public class HQLConvert {

  private static final Logger LOG = LoggerFactory.getLogger(HQLConvert.class);

  private Context context;
  private Map<String, String> params;

  public HQLConvert(Context context, Map<String, String> params) {
    this.context = context;
    this.params = params;
  }

  public void processReader(BufferedReader r) throws IOException, ParseException {
    String line;
    StringBuilder qsb = new StringBuilder();

    while ((line = r.readLine()) != null) {
      if (!isComment(line)) {
        qsb.append(line + "\n");
      }
    }
    processLine(StrSubstitutor.replace(qsb.toString(), params, "{$", "}"));
  }

  private boolean isComment(String line) {
    return line.startsWith("--") || line.startsWith("#");
  }

  private void processLine(String line) throws ParseException {
    String command = "";
    for (String oneCmd : line.split(";")) {

      //code copied from Hive CliDriver.processLine
      if (StringUtils.endsWith(oneCmd, "\\")) {
        command += StringUtils.chop(oneCmd) + ";";
        continue;
      } else {
        command += oneCmd;
      }

      if (StringUtils.isBlank(command)) {
        continue;
      }
      rewrite(oneCmd);
      command = "";
    }
  }

  private void rewrite(String oneCmd) throws ParseException {
    ParseDriver pd = new ParseDriver();

    ASTNode tree = pd.parse(oneCmd, context);
    tree = ParseUtils.findRootNonNullToken(tree);
    TokenRewriteStream ts = context.getTokenRewriteStream();

    PrePostOrderTraversor traversor = new PrePostOrderTraversor(new TestVisitor(ts));
    traversor.traverse(tree);
    LOG.info("Rewritten Query: {}", ts.toString());
  }
}