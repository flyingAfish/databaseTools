package org.flingAfish.database.hive;

import org.flingAfish.App;
import org.flingAfish.map.HiveColumnMap;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class HiveTable2Test {

    @Test
    public void genarateCrtTable() {
        String excelFile = "G:\\program\\Java\\JetBrains\\WorkSpace\\innerWorkSpace\\databaseTools\\src\\main\\resources\\template\\hive数据模型模板.xlsx";
        // String excelFile = "D:\\Program Files\\JetBrains\\workspace\\databaseTools\\src\\main\\resources\\template\\hive数据模型模板.xlsx";

        try {
            HiveColumnMap hiveColumnMap = App.parseColumnMap(excelFile);

            List<Map<String, Object>> modelMaps = App.getModelMaps(excelFile);

            Map<String, String> validColumnMap = App.getValidColumn(modelMaps, hiveColumnMap);

            HiveTable2 hiveTable = new HiveTable2();

            hiveTable.genarateCrtTable(modelMaps, validColumnMap);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}