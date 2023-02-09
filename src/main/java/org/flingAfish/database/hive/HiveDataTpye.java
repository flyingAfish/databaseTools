package org.flingAfish.database.hive;

public class HiveDataTpye {
    // primitive_type
    public static final String TINYINT   = "TINYINT";
    public static final String SMALLINT  = "SMALLINT";
    public static final String INT       = "INT";
    public static final String BIGINT    = "BIGINT";
    public static final String BOOLEAN   = "BOOLEAN";
    public static final String FLOAT     = "FLOAT";
    public static final String DOUBLE    = "DOUBLE";
    public static final String STRING    = "STRING";
    public static final String BINARY    = "BINARY";
    public static final String TIMESTAMP = "TIMESTAMP";
    public static final String DECIMAL   = "DECIMAL";
    public static final String DATE      = "DATE";
    public static final String VARCHAR   = "VARCHAR";
    public static final String CHAR      = "CHAR";
    public static final String ARRAY     = "ARRAY";
    public static final String MAP       = "MAP";
    public static final String STRUCT    = "STRUCT";
    public static final String UNIONTYPE = "UNIONTYPE";

    public static boolean isHiveDataType(String dataType){
        boolean isHiveDataType = false;
        switch (dataType) {
            case TINYINT   : {isHiveDataType = true;break;}
            case SMALLINT  : {isHiveDataType = true;break;}
            case INT       : {isHiveDataType = true;break;}
            case BIGINT    : {isHiveDataType = true;break;}
            case BOOLEAN   : {isHiveDataType = true;break;}
            case FLOAT     : {isHiveDataType = true;break;}
            case DOUBLE    : {isHiveDataType = true;break;}
            case STRING    : {isHiveDataType = true;break;}
            case BINARY    : {isHiveDataType = true;break;}
            case TIMESTAMP : {isHiveDataType = true;break;}
            case DECIMAL   : {isHiveDataType = true;break;}
            case DATE      : {isHiveDataType = true;break;}
            case VARCHAR   : {isHiveDataType = true;break;}
            case CHAR      : {isHiveDataType = true;break;}
            case ARRAY     : {isHiveDataType = true;break;}
            case MAP       : {isHiveDataType = true;break;}
            case STRUCT    : {isHiveDataType = true;break;}
            case UNIONTYPE : {isHiveDataType = true;break;}
        }
        return isHiveDataType;
    }

    public static boolean isHiveDataTypeIgnoreCase(String dataType){
        return isHiveDataType(dataType.toUpperCase());
    }
}
