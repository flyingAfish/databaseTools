package org.flingAfish.tools;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

public class ExcelToolTest {

    @Test
    public void excelExport() {
//        ExcelTool.excelExport("hive数据模型模板", );
    }

    @Test
    public void excelInport() throws IOException {
        String filePath = Paths.get(System.getProperty("user.dir"), "src", "main", "resources", "template", "hive数据模型模板.xlsx").toString();
        File absoluteFile = new File(filePath).getAbsoluteFile();
        System.out.println(absoluteFile);

        List<Map<String, Object>> maps = ExcelTool.excelInport(absoluteFile, 1);
        System.out.println(maps);
    }

    @Test
    public void excelSheetExport() {
        System.out.println(System.getProperty("java.class.path"));
        System.out.println(System.getProperty("user.dir"));

        Path path = Paths.get(System.getProperty("user.dir"), "src", "main", "resources", "template");

        System.out.println(path.getFileName());
        System.out.println(path.getFileSystem());
        System.out.println(path.getParent());
        System.out.println(path.getName(2));
        System.out.println(path.getRoot());
        System.out.println(path.getNameCount());
        System.out.println(path.toFile());
        System.out.println(path.endsWith("hive数据模型模板.xlsx"));
        System.out.println(path.toAbsolutePath());
    }
}