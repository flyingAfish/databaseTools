package org.flingAfish.database.hive;

import org.apache.commons.lang3.StringUtils;

import java.util.*;

public class HiveGenerateTable {
    private List<Map<String, Object>> modelMaps;
    private Map<String, String> validColumn;
    private Map<String, HiveTable> hiveTables;
    private Map<String, String> crtTableStatement; // tabnm, crtSql

    public HiveGenerateTable(List<Map<String, Object>> modelMaps, Map<String, String> validColumn) {
        this.modelMaps = modelMaps;
        this.validColumn = validColumn;
        hiveTables = new HashMap<>();
        crtTableStatement = new HashMap<>();
    }


    public Map<String, HiveTable> getHiveTables() {
        return hiveTables;
    }

    public Map<String, String> getCrtTableStatement() {
        return crtTableStatement;
    }


    public void genarateCrtTable(){
        for (Map.Entry<String, HiveTable> tableEntry : hiveTables.entrySet()) {
            String tabnm = tableEntry.getKey();
            HiveTable tabToken = tableEntry.getValue();
            StringBuilder builder = new StringBuilder();
            builder.append("CREATE ")
                    .append(nullToSpace(tabToken.getTableType()))
                    .append("TABLE IF NOT EXISTS ")
                    .append(nullToSpaceWithSuffix(tabToken.getDbName(), "."))
                    .append(nullToSpaceWithSuffix(tabToken.getTableName(),"("))
                    ;

            // 拼接字段
            for (int index = 0; index < tabToken.getColName().size(); index++) {
                String postFix = index == 0 ? "\n    `" : "\n   ,`";
                builder.append(nullToSpaceWithPostfix(tabToken.getColName().get(index),postFix)).append("`")
                        .append(nullToSpaceWithPostfix(tabToken.getDataType().get(index)," "))
                        .append(tabToken.getUnique().get(index) ? " UNIQUE" : "")
                        .append(tabToken.getNotNull().get(index) ? " NOT NULL" : "")
                        .append(" DEFAULT ")
                        .append(tabToken.getDefaultValue().get(index))
                        .append(" COMMENT '")
                        .append(nullToSpace(tabToken.getColComment().get(index)))
                        .append("'");
            }

            // constraint_specification
            builder.append("")
                    .append("\n")
                    .append(") ")
                    .append(tabToken.getTableComment() == null ? "" : "COMMENT '" + tabToken.getTableComment() + "'");

            // 拼接分区
            int partSize = tabToken.getPartitioned().values().size();
            for (int index = 0; index < partSize; index++) {
                String partition = tabToken.getPartitioned().get(index);
                if (partSize == 1) {
                    builder.append("\n").append("PARTITIONED BY (").append(partition).append(")");
                } else if (index == 0) {
                    builder.append("\n").append("PARTITIONED BY(").append(partition);
                } else if (index == partSize - 1) {
                    builder.append(", ").append(partition).append(")");
                } else {
                    builder.append(", ").append(partition);
                }
            }

            // 拼接分桶
            int clusterSize = tabToken.getClustered().values().size();
            for (int i = 0; i < clusterSize; i++) {
                String cluster = tabToken.getClustered().get(i);
                if (clusterSize == 1) {
                    builder.append("\n").append("CLUSTERED BY (").append(cluster).append(")");
                } else if (i == 0) {
                    builder.append("\n").append("CLUSTERED BY (").append(cluster);
                } else if (i == clusterSize - 1) {
                    builder.append(", ").append(cluster).append(")");
                } else {
                    builder.append(", ").append(cluster);
                }
            }

            if (clusterSize > 0) {
                // 拼接分桶排序
                int sortedSize = tabToken.getSorted().values().size();
                for (int index = 0; index < sortedSize; index++) {
                    String sorter = tabToken.getSorted().get(index);
                    if (sortedSize == 1) {
                        builder.append(" ").append("SORTED BY (").append(sorter).append(")");
                    } else if (index == 0) {
                        builder.append(" ").append("SORTED BY (").append(sorter);
                    } else if (index == sortedSize - 1) {
                        builder.append(", ").append(sorter).append(")");
                    } else {
                        builder.append(", ").append(sorter);
                    }
                }

                // 拼接分桶数
                builder.append(" INTO ");
                if (tabToken.getNumBuckets() != null && tabToken.getNumBuckets() > 0) {
                    builder.append(tabToken.getNumBuckets());
                } else {
                    builder.append(1);
                }
                builder.append(" BUCKETS");
            }

            // row_format
            if (tabToken.getRowFormat() != null) {
                builder.append("\n")
                        .append(tabToken.getRowFormat());
            }

            // 拼接存储格式
            if (tabToken.getStored() != null) {
                builder.append("\n")
                        .append("STORED AS ")
                        .append(tabToken.getStored());
            }

            // 拼接hdfs存储位置
            if (tabToken.getLocation() != null) {
                builder.append("\n")
                        .append("LOCATION '")
                        .append(tabToken.getLocation())
                        .append("'");
            }

            // 拼接表属性
            if (tabToken.getTblproperties() != null) {
                builder.append("\n")
                        .append("TBLPROPERTIES (")
                        .append(tabToken.getLocation())
                        .append(")");
            }

            this.crtTableStatement.put(tabnm, builder.append(";").toString());
        }
    }

    public void parseTable() {
        HashMap<String, String> previouse = new HashMap<>();
        HashMap<String, String> current = new HashMap<>();
        HiveTable hiveTable = new HiveTable();
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
            if (!checkAttr(HiveTableAttr.tableName, current.get(HiveTableAttr.tableName))) {
                break;
            }

            checkAttr(HiveTableAttr.tableType, current.get(HiveTableAttr.tableType));
            checkAttr(HiveTableAttr.colName, current.get(HiveTableAttr.colName));
            checkAttr(HiveTableAttr.dataType, current.get(HiveTableAttr.dataType));
            checkAttr(HiveTableAttr.defaultValue, current.get(HiveTableAttr.defaultValue));
            checkAttr(HiveTableAttr.primaryKey, current.get(HiveTableAttr.primaryKey));
            checkAttr(HiveTableAttr.unique, current.get(HiveTableAttr.unique));
            checkAttr(HiveTableAttr.notNull, current.get(HiveTableAttr.notNull));
            checkAttr(HiveTableAttr.defaultValue, current.get(HiveTableAttr.defaultValue));
            checkAttr(HiveTableAttr.check, current.get(HiveTableAttr.check));
            checkAttr(HiveTableAttr.partitioned, current.get(HiveTableAttr.partitioned));
            checkAttr(HiveTableAttr.clustered, current.get(HiveTableAttr.clustered));
            checkAttr(HiveTableAttr.numBuckets, current.get(HiveTableAttr.numBuckets));
            checkAttr(HiveTableAttr.sorted, current.get(HiveTableAttr.sorted));
            checkAttr(HiveTableAttr.skewed, current.get(HiveTableAttr.skewed));
            checkAttr(HiveTableAttr.stored, current.get(HiveTableAttr.stored));
            checkAttr(HiveTableAttr.rowFormat, current.get(HiveTableAttr.rowFormat));
            checkAttr(HiveTableAttr.location, current.get(HiveTableAttr.location));
            checkAttr(HiveTableAttr.tblproperties, current.get(HiveTableAttr.tblproperties));

            // 建立新表
            if (!Objects.equals(hiveTable.getTableName(), current.get(HiveTableAttr.tableName))) {
                hiveTable = new HiveTable();
            }

            hiveTable.setOrDefaultDbName(current.get(HiveTableAttr.dbName),hiveTable.getDbName());
            hiveTable.setTableName(current.get(HiveTableAttr.tableName));
            hiveTable.setOrDefaultTableComment(current.get(HiveTableAttr.tableComment), hiveTable.getTableComment());

            // 赋值分区字段 并 移除分区字段信息
            TreeMap<Integer, String> partitioned = hiveTable.getPartitioned();
            if (sortTableAttr(current, partitioned, HiveTableAttr.partitioned)) {
                StringBuilder partBuilder = new StringBuilder();
                partBuilder.append(current.get(HiveTableAttr.colName))
                        .append(nullToSpaceWithPostfix(current.get(HiveTableAttr.dataType), " "))
                        .append(nullToSpaceWithPostfix(current.get(HiveTableAttr.colComment), " COMMENT '"))
                        .append("'");
                partitioned.put(partitioned.lastKey(), partBuilder.toString());
            } else {
                hiveTable.getColName().add(current.get(HiveTableAttr.colName));
                hiveTable.getDataType().add(current.get(HiveTableAttr.dataType));
                hiveTable.getDefaultValue().add(current.get(HiveTableAttr.defaultValue));
                hiveTable.getColComment().add(current.get(HiveTableAttr.colComment));
            }

            TreeMap<Integer, String> primaryKey = hiveTable.getPrimaryKey();
            sortTableAttr(current, primaryKey, HiveTableAttr.primaryKey);

            hiveTable.getUnique().add("Y".equalsIgnoreCase(current.get(HiveTableAttr.unique)));
            hiveTable.getNotNull().add("Y".equalsIgnoreCase(current.get(HiveTableAttr.notNull)));
            hiveTable.getCheck().add(current.get(HiveTableAttr.check));


            TreeMap<Integer, String> clustered = hiveTable.getClustered();
            sortTableAttr(current, clustered, HiveTableAttr.clustered);

            hiveTable.setOrDefaultNumBuckets(StringUtils.isNumeric(current.get(HiveTableAttr.numBuckets)) ? Integer.parseInt(current.get(HiveTableAttr.numBuckets)) : null
                    , hiveTable.getNumBuckets());

            TreeMap<Integer, String> sorted = hiveTable.getSorted();
            sortTableAttr(current, sorted, HiveTableAttr.sorted);

            // skewed 暂时不支持
            hiveTable.setSkewed(null);

            hiveTable.setOrDefaultRowFormat(current.get(HiveTableAttr.rowFormat),hiveTable.getRowFormat());
            hiveTable.setOrDefaultStored(current.get(HiveTableAttr.stored),hiveTable.getStored());
            hiveTable.setOrDefaultLocation(current.get(HiveTableAttr.location),hiveTable.getLocation());
            hiveTable.setOrDefaultTblproperties(current.get(HiveTableAttr.tblproperties),hiveTable.getTblproperties());

            hiveTables.put(current.get(HiveTableAttr.tableName), hiveTable);
        }
    }

    private boolean sortTableAttr(HashMap<String, String> souAttr, TreeMap<Integer, String> tarAttr, String tableAttr) {
        if (souAttr.get(tableAttr) == null)
            return false;

        String[] split = souAttr.get(tableAttr).split("-");
        if(split.length > 1 && StringUtils.isNumeric(split[1])){
            tarAttr.put(Integer.valueOf(split[1]), souAttr.get(HiveTableAttr.colName));
            return true;
        } else if ("Y".equalsIgnoreCase(split[0])) {
            int lastKey = tarAttr.lastEntry() == null ? 0 : tarAttr.lastKey() + 1;
            tarAttr.put(lastKey, souAttr.get(HiveTableAttr.colName));
            return true;
        }else if ("ASC".equalsIgnoreCase(split[0]) || "DESC".equalsIgnoreCase(split[0])) {
            int lastKey = tarAttr.lastEntry() == null ? 0 : tarAttr.lastKey() + 1;
            tarAttr.put(lastKey, souAttr.get(HiveTableAttr.colName) + " " + split[0].toUpperCase());
            return true;
        }

        return false;
    }

    private String parseAttr(String attr, Map<String, Object> modelMap, Map<String, String> validColumn){
        return (validColumn.get(attr) == null || modelMap.get(validColumn.get(attr)) == null) ? null : modelMap.get(validColumn.get(attr)).toString();
    }

    private boolean checkAttr(String attr, String value) {
        if (attr == null) return false;

        switch (attr) {
            case HiveTableAttr.dbName:
            case HiveTableAttr.tableName:
            case HiveTableAttr.colName:
                return value != null && !"".equals(value.trim());
            case HiveTableAttr.tableComment:
            case HiveTableAttr.colComment:
            case HiveTableAttr.primaryKey:
                return value != null;
            case HiveTableAttr.dataType:
                return value != null && HiveDataTpye.isHiveDataTypeIgnoreCase(value.split("<")[0].split("\\(")[0].trim());
            case HiveTableAttr.tableType:
                return value == null || "TEMPORARY".equals(value.trim().toUpperCase()) || "EXTERNAL".equals(value.trim().toUpperCase());
        }

        return false;
    }

    private String nullToSpace(String value){
        return value == null ? "" : value;
    }

    private String nullToSpaceWithSuffix(String value, String suffix){
        return value == null ? "" : value + suffix;
    }

    private String nullToSpaceWithPostfix(String value, String postfix){
        return value == null ? "" : postfix + value;
    }
}
