package com.yhd.hive;


import com.google.common.collect.Lists;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class HiveScriptRewrite {

    private String scriptFile;

    public HiveScriptRewrite(String scriptFile, Map<String, String> params, List<String> excludes) {
        this.scriptFile = scriptFile;
    }

    private void getStatements(){
        List<String> commands = Lists.newArrayList();
        BufferedReader in = null;
        try {
            in = new BufferedReader(new FileReader(scriptFile));
            String line;
            StringBuilder command = new StringBuilder();
            while ((line = in.readLine()) != null) {
                if (skippableLine(line) || excludeLine(line)) {
                    continue;
                }
                if (line.endsWith(";")) {
                    command.append(replaceParams(line.replace(";", "")));
                    commands.add(command.toString());
                    command = new StringBuilder();
                }
                else {
                    command.append(replaceParams(line));
                    //need to make sure there is a space between lines
                    command.append(" ");
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        //return commands;
    }

    private boolean excludeLine(String line) {
        return false;
    }

    private boolean skippableLine(String line) {
        return  false;
    }

    private String replaceParams(String line) {
        return line;
    }
}
