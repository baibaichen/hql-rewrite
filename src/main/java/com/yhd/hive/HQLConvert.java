package com.yhd.hive;

import java.io.BufferedReader;
import java.io.IOException;

public class HQLConvert {

    public void processReader(BufferedReader r) throws IOException {
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

    private void processLine(String line){
        String command = "";
        for (String oneCmd : line.split(";")) {
            System.out.print(oneCmd);
        }
    }
}