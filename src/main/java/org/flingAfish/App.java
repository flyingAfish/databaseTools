package org.flingAfish;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.flingAfish.database.hive.HiveGenerateTable;
import org.flingAfish.database.hive.HiveTableAttr;
import org.flingAfish.map.HiveColumnMap;
import org.flingAfish.tools.DonwloadFile;
import org.flingAfish.tools.ExcelTool;
import org.flingAfish.tools.ParameterTool;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Hello world!
 *
 */
public class App {
    public static final Options OPTIONS;
    public static PrintStream out;
    public static PrintStream err;

    static {
        OPTIONS = new Options();
        Option help = OptionBuilder.withLongOpt("help")
                .withDescription("Show this help string")
                .create('h');

        Option ncol = OptionBuilder.withLongOpt("no-color")
                .withDescription("Disable color output even if supported")
                .create();

        Option debg = OptionBuilder.withLongOpt("debug")
                .withDescription("Disable color output even if supported")
                .create();

        OPTIONS.addOption(help);
        OPTIONS.addOption(ncol);
        OPTIONS.addOption(debg);
    }

    public static HiveColumnMap parseColumnMap(String excelFile) throws IOException {
        List<Map<String, Object>> columeMaps = ExcelTool.excelInport(new File(excelFile), 1);
        if (columeMaps == null || columeMaps.size() < 1) {
            return new HiveColumnMap();
        }

        Map<String, Object> bootMap = columeMaps.get(0);
        return new HiveColumnMap(
                (String)bootMap.get(HiveTableAttr.dbName)
                ,(String)bootMap.get(HiveTableAttr.tableName)
                ,(String)bootMap.get(HiveTableAttr.tableComment)
                ,(String)bootMap.get(HiveTableAttr.colName)
                ,(String)bootMap.get(HiveTableAttr.colComment)
                ,(String)bootMap.get(HiveTableAttr.dataType)
                ,(String)bootMap.get(HiveTableAttr.tableType)
                ,(String)bootMap.get(HiveTableAttr.primaryKey)
                ,(String)bootMap.get(HiveTableAttr.unique)
                ,(String)bootMap.get(HiveTableAttr.notNull)
                ,(String)bootMap.get(HiveTableAttr.defaultValue)
                ,(String)bootMap.get(HiveTableAttr.check)
                ,(String)bootMap.get(HiveTableAttr.partitioned)
                ,(String)bootMap.get(HiveTableAttr.clustered)
                ,(String)bootMap.get(HiveTableAttr.numBuckets)
                ,(String)bootMap.get(HiveTableAttr.sorted)
                ,(String)bootMap.get(HiveTableAttr.skewed)
                ,(String)bootMap.get(HiveTableAttr.stored)
                ,(String)bootMap.get(HiveTableAttr.rowFormat)
                ,(String)bootMap.get(HiveTableAttr.location)
                ,(String)bootMap.get(HiveTableAttr.tblproperties)
                ,new HashMap<>()
        );
    }

    public static List<Map<String, Object>> getModelMaps(String excelFile) throws IOException {
        return ExcelTool.excelInport(new File(excelFile), 2);
    }

    public static Map<String, String> getValidColumn(List<Map<String, Object>> modelMaps, HiveColumnMap columnMap) throws IOException {
        if (modelMaps == null || modelMaps.size() < 1) {
            return columnMap.getValidColumns();
        }

        for (String modelColumn : modelMaps.get(0).keySet()) {
            columnMap.checkValidColumn(modelColumn);
        }

        return columnMap.getValidColumns();
    }

    public static void parseTableInfo(List<Map<String, Object>> modelMaps, Map<String, Integer> validColumn){
        Integer integer = validColumn.get(HiveTableAttr.dbName);
    }

    public static void main( String[] args ) {
        //String excelFile = "G:\\program\\Java\\JetBrains\\WorkSpace\\innerWorkSpace\\databaseTools\\src\\main\\resources\\template\\hive数据模型模板.xlsx";
        //String excelFile = "D:\\Program Files\\JetBrains\\workspace\\databaseTools\\src\\main\\resources\\template\\hive数据模型模板.xlsx";
        ParameterTool tool = ParameterTool.fromArgs(args);
        String excelFile = tool.getRequired("excelModel");
        String exportFile = tool.getRequired("exportFile");

        String importDir = Paths.get(excelFile).toFile().exists() ?
                excelFile : Paths.get(System.getProperty("user.dir"),
                "src", "main", "resources", "template", excelFile).toString();
        String exportDir = Paths.get(System.getProperty("user.dir"),
                "src", "main", "resources", "testData", exportFile).toString();
        exportDir = Paths.get(exportDir).getParent().toFile().exists() ? exportDir : exportFile;

        try {
            HiveColumnMap hiveColumnMap = parseColumnMap(importDir);

            List<Map<String, Object>> modelMaps = getModelMaps(importDir);

            Map<String, String> validColumnMap = getValidColumn(modelMaps, hiveColumnMap);

            HiveGenerateTable hiveGenerateTable = new HiveGenerateTable(modelMaps, validColumnMap);

            hiveGenerateTable.parseTable();

            hiveGenerateTable.genarateCrtTable();

            for (Map.Entry<String, String> entry : hiveGenerateTable.getCrtTableStatement().entrySet()) {
                System.out.println(entry.getKey() + " : " + entry.getValue());
            }


            DonwloadFile.exportModelFile(exportDir, hiveGenerateTable);

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
