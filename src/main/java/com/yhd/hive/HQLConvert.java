package com.yhd.hive;

import com.yhd.hive.queryparser.PrePostOrderTraversor;
import com.yhd.hive.queryparser.TestVisitor;
import org.antlr.runtime.TokenRewriteStream;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;

public class HQLConvert {

    private static final Logger LOG = LoggerFactory.getLogger(HQLConvert.class);

    private  Context context;

    public HQLConvert(Context context){
        this.context = context;
    }
    public void processReader(BufferedReader r) throws IOException, ParseException {
        String line;
        StringBuilder qsb = new StringBuilder();

        while ((line = r.readLine()) != null) {
            // Skipping through comments
            if (!line.startsWith("--")) {
                qsb.append(line + "\n");
            }
        }
        processLine(qsb.toString());
    }

    private void processLine(String line) throws ParseException {
        String command = "";
        for (String oneCmd : line.split(";")) {
            rewrite(oneCmd);
        }
    }

    private void rewrite(String oneCmd) throws ParseException {
        ParseDriver pd = new ParseDriver();

        ASTNode tree = pd.parse(oneCmd,context);
        tree = ParseUtils.findRootNonNullToken(tree);
        TokenRewriteStream ts = context.getTokenRewriteStream();

        PrePostOrderTraversor traversor = new PrePostOrderTraversor(new TestVisitor(ts));
        traversor.traverse(tree);
        LOG.info("Rewritten Query: {}", ts.toString());
    }
}