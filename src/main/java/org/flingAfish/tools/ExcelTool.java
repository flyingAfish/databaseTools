package org.flingAfish.tools;


import cn.afterturn.easypoi.excel.ExcelExportUtil;
import cn.afterturn.easypoi.excel.ExcelImportUtil;
import cn.afterturn.easypoi.excel.entity.ExportParams;
import cn.afterturn.easypoi.excel.entity.ImportParams;
import cn.afterturn.easypoi.excel.entity.enmus.ExcelType;
import cn.afterturn.easypoi.excel.export.ExcelExportService;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.flingAfish.entity.Entity;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class ExcelTool {
    public static       int    USE_SXSSF_LIMIT = 100000;

    public static void excelExport(String title, String sheetName, List<Entity> dataSet, String fileName) throws IOException {
        Workbook workbook = ExcelExportUtil.exportExcel(new ExportParams(title,sheetName), Entity.class, dataSet);
        FileOutputStream fos = new FileOutputStream(fileName);
        workbook.write(fos);
        fos.close();
    }

    public static void ExcelSheetExport(List<Map<String, Object>> list, ExcelType type, String fileName) throws IOException {
        Workbook workbook = getWorkbook(type, 0);

        for (Map<String, Object> map : list) {
            ExcelExportService service = new ExcelExportService();
            service.createSheet(workbook, (ExportParams) map.get("title")
                    , (Class<?>) map.get("entity")
                    , (Collection<?>) map.get("data"));
        }
        FileOutputStream fos = new FileOutputStream(fileName);
        workbook.write(fos);
        fos.close();
    }

    public static List<Map<String, Object>> excelInport(File excelFileName, int startSheetIndex) throws IOException {
        ImportParams params = new ImportParams();
        params.setTitleRows(0);
        params.setHeadRows(1);
        params.setSheetNum(1);
        params.setStartSheetIndex(startSheetIndex);
        long start = new Date().getTime();
        List<Map<String, Object>> dataSet = ExcelImportUtil.importExcel(excelFileName, Map.class, params);
        //System.out.println(new Date().getTime() - start);
        //System.out.println(JSONArray.toJSONString(dataSet));

        return dataSet;
    }

    private static Workbook getWorkbook(ExcelType type, int size) {
        if (ExcelType.HSSF.equals(type)) {
            return new HSSFWorkbook();
        } else if (size < USE_SXSSF_LIMIT) {
            return new XSSFWorkbook();
        } else {
            return new SXSSFWorkbook();
        }
    }
}
