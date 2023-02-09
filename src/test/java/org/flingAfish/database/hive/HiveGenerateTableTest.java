package org.flingAfish.database.hive;

import org.flingAfish.App;
import org.flingAfish.map.HiveColumnMap;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class HiveGenerateTableTest {

    @Test
    public void testGenarateCrtTable() {
        String excelFile = "G:\\program\\Java\\JetBrains\\WorkSpace\\innerWorkSpace\\databaseTools\\src\\main\\resources\\template\\hive数据模型模板.xlsx";
        // String excelFile = "D:\\Program Files\\JetBrains\\workspace\\databaseTools\\src\\main\\resources\\template\\hive数据模型模板.xlsx";

        try {
            HiveColumnMap hiveColumnMap = App.parseColumnMap(excelFile);

            List<Map<String, Object>> modelMaps = App.getModelMaps(excelFile);

            Map<String, String> validColumnMap = App.getValidColumn(modelMaps, hiveColumnMap);

            HiveGenerateTable hiveGenerateTable = new HiveGenerateTable(modelMaps, validColumnMap);

            hiveGenerateTable.parseTable();

            hiveGenerateTable.genarateCrtTable();

            //Map<String, HiveTable> hiveTables = hiveGenerateTable.getHiveTables();
            //for (Map.Entry<String, HiveTable> entry : hiveTables.entrySet()) {
            //    System.out.println(entry);
            //}

            Map<String, String> crtTableStatement = hiveGenerateTable.getCrtTableStatement();
            for (Map.Entry<String, String> entry : crtTableStatement.entrySet()) {
                //System.out.println(entry.getKey());
                //System.out.println(entry.getValue());
                System.out.println(entry.getKey() + " : " + entry.getValue());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}