package org.flingAfish.tools;

import org.flingAfish.database.hive.HiveGenerateTable;

import java.io.*;
import java.util.Map;

public class DonwloadFile {
    public static void exportModelFile(String exportDir, HiveGenerateTable tableData){
        BufferedWriter bufferedWriter = null;
        try {
            bufferedWriter = new BufferedWriter(new FileWriter(exportDir));

            Map<String, String> crtTableStatement = tableData.getCrtTableStatement();
            for (String tabnm : crtTableStatement.keySet()) {
                bufferedWriter.write("-- " + tabnm);
                bufferedWriter.newLine();
            }

            bufferedWriter.newLine();
            for (String crtTableSql : crtTableStatement.values()) {
                bufferedWriter.write(crtTableSql);
                bufferedWriter.newLine();
                bufferedWriter.newLine();
            }

            bufferedWriter.flush();

        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (bufferedWriter != null) {
                    bufferedWriter.close();
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
