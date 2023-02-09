package org.flingAfish.database.hive;

import org.apache.commons.lang3.StringUtils;

import java.util.*;

public class HiveTable2 {
    private String dbName;
    private String tableName;
    private String tableComment;
    private String colName;
    private String colComment;
    private String dataType;
    private String tableType;
    private String primaryKey;
    private String unique;
    private String notNull;
    private String defaultValue;
    private String check;
    private TreeMap<Integer, String> partitioned; // 分区顺序,分区字段
    private String clustered;
    private String numBuckets;
    private String sorted;
    private String skewed;
    private String stored;
    private String rowFormat;
    private String location;
    private String tblproperties;

    private boolean isFirstColumn;
    private Map<String, String> crtTableStatement;

    public HiveTable2(){
        isFirstColumn = false;
        crtTableStatement = new HashMap<>();
        partitioned = new TreeMap<Integer, String>();
    }

    public void genarateCrtTable(List<Map<String, Object>> modelMaps, Map<String, String> validColumn){
        HashMap<String, String> previouse = new HashMap<>();
        HashMap<String, String> current = new HashMap<>();
        boolean isEndTable = false;
        for (Map<String, Object> modelMap : modelMaps) {
            current.clear();
            current.put(HiveTableAttr.dbName, parseAttr(HiveTableAttr.dbName, modelMap, validColumn));
            current.put(HiveTableAttr.tableName, parseAttr(HiveTableAttr.tableName, modelMap, validColumn));
            current.put(HiveTableAttr.tableComment, parseAttr(HiveTableAttr.tableComment, modelMap, validColumn));
            current.put(HiveTableAttr.colName, parseAttr(HiveTableAttr.colName, modelMap, validColumn));
            current.put(HiveTableAttr.colComment, parseAttr(HiveTableAttr.colComment, modelMap, validColumn));
            current.put(HiveTableAttr.dataType, parseAttr(HiveTableAttr.dataType, modelMap, validColumn));
            current.put(HiveTableAttr.tableType, parseAttr(HiveTableAttr.tableType, modelMap, validColumn));
            current.put(HiveTableAttr.primaryKey, parseAttr(HiveTableAttr.primaryKey, modelMap, validColumn));
            current.put(HiveTableAttr.unique, parseAttr(HiveTableAttr.unique, modelMap, validColumn));
            current.put(HiveTableAttr.notNull, parseAttr(HiveTableAttr.notNull, modelMap, validColumn));
            current.put(HiveTableAttr.defaultValue, parseAttr(HiveTableAttr.defaultValue, modelMap, validColumn));
            current.put(HiveTableAttr.check, parseAttr(HiveTableAttr.check, modelMap, validColumn));
            current.put(HiveTableAttr.partitioned, parseAttr(HiveTableAttr.partitioned, modelMap, validColumn));
            current.put(HiveTableAttr.clustered, parseAttr(HiveTableAttr.clustered, modelMap, validColumn));
            current.put(HiveTableAttr.numBuckets, parseAttr(HiveTableAttr.numBuckets, modelMap, validColumn));
            current.put(HiveTableAttr.sorted, parseAttr(HiveTableAttr.sorted, modelMap, validColumn));
            current.put(HiveTableAttr.skewed, parseAttr(HiveTableAttr.skewed, modelMap, validColumn));
            current.put(HiveTableAttr.stored, parseAttr(HiveTableAttr.stored, modelMap, validColumn));
            current.put(HiveTableAttr.rowFormat, parseAttr(HiveTableAttr.rowFormat, modelMap, validColumn));
            current.put(HiveTableAttr.location, parseAttr(HiveTableAttr.location, modelMap, validColumn));
            current.put(HiveTableAttr.tblproperties, parseAttr(HiveTableAttr.tblproperties, modelMap, validColumn));

            // tableName为空则结束sql拼接
            if (isEndTable) {
                break;
            }
            if (!checkAttr(HiveTableAttr.tableName, current.get(HiveTableAttr.tableName))) {
                isEndTable = true;
            }

            checkAttr(HiveTableAttr.tableType, current.get(HiveTableAttr.tableType));
            checkAttr(HiveTableAttr.colName, current.get(HiveTableAttr.colName));
            checkAttr(HiveTableAttr.dataType, current.get(HiveTableAttr.dataType));
            checkAttr(HiveTableAttr.defaultValue, current.get(HiveTableAttr.defaultValue));

            // 当前行和前一行表名相同，则拼接字段，否则建立新表
            // System.out.println(current.getOrDefault(HiveTableAttr.tableName, " ") + "==" + previouse.get(HiveTableAttr.tableName));
            // System.out.println();
            if (Objects.equals(current.get(HiveTableAttr.tableName), previouse.get(HiveTableAttr.tableName))) {
                String colSql = crtTableStatement.get(current.getOrDefault(HiveTableAttr.tableName, ""))
                        + "\n     ,"
                        + current.getOrDefault(HiveTableAttr.colName, " ")
                        + " "
                        + current.getOrDefault(HiveTableAttr.dataType, " ")
                        + " "
                        + ("Y".equalsIgnoreCase(current.get(HiveTableAttr.notNull)) ? "NOT NULL" : "")
                        + " "
                        + "DEFAULT " + current.get(HiveTableAttr.defaultValue)
                        + " "
                        + "COMMENT '" + current.getOrDefault(HiveTableAttr.colComment, "") + "'"
                        ;

                // 字段外属性赋值
                this.tableComment = "".equals(previouse.getOrDefault(HiveTableAttr.tableComment, "").trim()) ?
                        current.getOrDefault(HiveTableAttr.tableComment, this.tableComment)
                        : previouse.get(HiveTableAttr.tableComment);

                crtTableStatement.put(previouse.get(HiveTableAttr.tableName), colSql);
                isFirstColumn = false;
            } else { // 新建表
                isFirstColumn = true;

                if (current.get(HiveTableAttr.tableName) != null) {
                    // 建立新表，同时旧表封尾
                    String sql = "CREATE "
                            + (current.get(HiveTableAttr.tableType) == null ? "" : current.get(HiveTableAttr.tableType))
                            + "TABLE IF NOT EXISTS "
                            + (checkAttr(HiveTableAttr.dbName, current.get(HiveTableAttr.dbName)) ? current.get(HiveTableAttr.dbName) + "." : "")
                            + current.get(HiveTableAttr.tableName)
                            + "("
                            + "\n     "
                            + current.getOrDefault(HiveTableAttr.colName, " ")
                            + " "
                            + current.getOrDefault(HiveTableAttr.dataType, " ")
                            + " "
                            + ("Y".equalsIgnoreCase(current.get(HiveTableAttr.notNull)) ? "NOT NULL" : "")
                            + " "
                            + "DEFAULT " + current.get(HiveTableAttr.defaultValue)
                            + " "
                            + "COMMENT '" + current.getOrDefault(HiveTableAttr.colComment, "") + "'";

                    crtTableStatement.put(current.get(HiveTableAttr.tableName), sql);
                }


            }

            // 分区赋值
            String partNumericSql = previouse.getOrDefault(HiveTableAttr.colName, "")
                    + " "
                    + previouse.getOrDefault(HiveTableAttr.dataType, " ")
                    + " "
                    + "COMMENT '" + previouse.getOrDefault(HiveTableAttr.colComment, "") + "'"
                    ;

            //String partNonNumericSql =
            if(StringUtils.isNumeric(previouse.get(HiveTableAttr.partitioned))){
                this.partitioned.put(Integer.valueOf(previouse.get(HiveTableAttr.partitioned)), partNumericSql);
            } else if ("Y".equalsIgnoreCase(previouse.get(HiveTableAttr.partitioned))) {
                int lastKey = this.partitioned.lastEntry() == null ? 0 : this.partitioned.lastKey() + 1;
                this.partitioned.put(lastKey, partNumericSql);
            }

            // 建表后半部分
            if (previouse.get(HiveTableAttr.tableName) != null && isFirstColumn) {
                // 分区编入
                StringBuilder partitionSql = new StringBuilder("PARTITIONED BY (");
                Object[] partitions = this.partitioned.values().toArray();
                for (int index = 0; index < partitions.length; index++) {
                    partitionSql.append(index == 0 ? partitions[index] : "," + partitions[index]);
                }
                partitionSql.append(")");
                String tailTableSql = crtTableStatement.get(previouse.get(HiveTableAttr.tableName))
                        + ("")  // 约束
                        + "\n)\n"
                        + "" // tab_comment
                        + partitionSql
                        ;
                crtTableStatement.put(previouse.get(HiveTableAttr.tableName), tailTableSql);
                this.partitioned.clear();
            }

            previouse.clear();
            previouse.putAll(current);
            //System.out.println(crtTableStatement);
        }

        for (String value : crtTableStatement.values()) {
            System.out.println(value);
            System.out.println();
        }
    }

    private void crtTableOperation(){

    }

    private String parseAttr(String attr, Map<String, Object> modelMap, Map<String, String> validColumn){
        return (validColumn.get(attr) == null || modelMap.get(validColumn.get(attr)) == null) ? null : modelMap.get(validColumn.get(attr)).toString();
    }

    private boolean checkAttr(String attr, String value) {
        if (attr == null) return false;

        if (attr.equals(HiveTableAttr.dbName)) {
            return value == null || value.trim() == "" ? false : true;
        } else if (attr.equals(HiveTableAttr.tableName)) {
            return value == null || value.trim() == "" ? false : true;
        } else if (attr.equals(attr.equals(HiveTableAttr.tableComment))) {
            return value == null ? false : true;
        } else if (attr.equals(attr.equals(HiveTableAttr.colName))) {
            return value == null || value.trim() == "" ? false : true;
        } else if (attr.equals(attr.equals(HiveTableAttr.colComment))) {
            return value == null ? false : true;
        } else if (attr.equals(attr.equals(HiveTableAttr.dataType))) {
            return value != null && HiveDataTpye.isHiveDataTypeIgnoreCase(value.split("<")[0].split("\\(")[0].trim()) ? true : false;
        } else if (attr.equals(attr.equals(HiveTableAttr.tableType))) {
            return value == null ||
                    (value != null &&
                            (value.trim().toUpperCase() == "TEMPORARY" ||
                                    value.trim().toUpperCase() == "EXTERNAL")) ? true : false;
        } else if (attr.equals(attr.equals(HiveTableAttr.primaryKey))) {
            return value == null ? false : true;
        }

        return false;
    }

    private void processError(){

    }

}
