package org.flingAfish.database.hive;

import org.junit.Test;

public class HiveDataTpyeTest {

    @Test
    public void isHiveDataType() {
        System.out.println(HiveDataTpye.isHiveDataType("tinyint"));
        System.out.println(HiveDataTpye.isHiveDataType("TINYINT"));
        System.out.println(HiveDataTpye.isHiveDataType("SMALLINT"));
        System.out.println(HiveDataTpye.isHiveDataType("INT"));
        System.out.println(HiveDataTpye.isHiveDataType("BIGINT"));
        System.out.println(HiveDataTpye.isHiveDataType("BOOLEAN"));
        System.out.println(HiveDataTpye.isHiveDataType("FLOAT"));
        System.out.println(HiveDataTpye.isHiveDataType("DOUBLE"));
        System.out.println(HiveDataTpye.isHiveDataType("STRING"));
        System.out.println(HiveDataTpye.isHiveDataType("BINARY"));
        System.out.println(HiveDataTpye.isHiveDataType("TIMESTAMP"));
        System.out.println(HiveDataTpye.isHiveDataType("DECIMAL"));
        System.out.println(HiveDataTpye.isHiveDataType("DATE"));
        System.out.println(HiveDataTpye.isHiveDataType("VARCHAR"));
        System.out.println(HiveDataTpye.isHiveDataType("CHAR"));
        System.out.println(HiveDataTpye.isHiveDataType("ARRAY"));
        System.out.println(HiveDataTpye.isHiveDataType("MAP"));
        System.out.println(HiveDataTpye.isHiveDataType("STRUCT"));
        System.out.println(HiveDataTpye.isHiveDataType("UNIONTYPE"));
    }

    @Test
    public void isHiveDataTypeIgnoreCase() {
        System.out.println(HiveDataTpye.isHiveDataTypeIgnoreCase("tinyint"));
    }
}