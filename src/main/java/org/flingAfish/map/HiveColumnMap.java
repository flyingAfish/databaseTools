package org.flingAfish.map;

import lombok.*;
import org.flingAfish.database.hive.HiveTableAttr;

import java.util.Map;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class HiveColumnMap {
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
    private String partitioned;
    private String clustered;
    private String numBuckets;
    private String sorted;
    private String skewed;
    private String stored;
    private String rowFormat;
    private String location;
    private String tblproperties;

    private Map<String, String> validColumns;

    public void checkValidColumn(String colName){
        if (colName == null) {
            return;
        }

        if (colName.equals(dbName)) {
            validColumns.put(HiveTableAttr.dbName, colName);
        } else if (colName.equals(tableName)) {
            validColumns.put(HiveTableAttr.tableName, colName);
        } else if (colName.equals(tableComment)) {
            validColumns.put(HiveTableAttr.tableComment, colName);
        } else if (colName.equals(this.colName)) {
            validColumns.put(HiveTableAttr.colName, colName);
        } else if (colName.equals(colComment)) {
            validColumns.put(HiveTableAttr.colComment, colName);
        } else if (colName.equals(dataType)) {
            validColumns.put(HiveTableAttr.dataType, colName);
        }else if (colName.equals(tableType)) {
            validColumns.put(HiveTableAttr.tableType, colName);
        } else if (colName.equals(primaryKey)) {
            validColumns.put(HiveTableAttr.primaryKey, colName);
        } else if (colName.equals(unique)) {
            validColumns.put(HiveTableAttr.unique, colName);
        } else if (colName.equals(notNull)) {
            validColumns.put(HiveTableAttr.notNull, colName);
        } else if (colName.equals(defaultValue)) {
            validColumns.put(HiveTableAttr.defaultValue, colName);
        } else if (colName.equals(check)) {
            validColumns.put(HiveTableAttr.check, colName);
        } else if (colName.equals(partitioned)) {
            validColumns.put(HiveTableAttr.partitioned, colName);
        } else if (colName.equals(clustered)) {
            validColumns.put(HiveTableAttr.clustered, colName);
        } else if (colName.equals(numBuckets)) {
            validColumns.put(HiveTableAttr.numBuckets, colName);
        } else if (colName.equals(sorted)) {
            validColumns.put(HiveTableAttr.sorted, colName);
        } else if (colName.equals(skewed)) {
            validColumns.put(HiveTableAttr.skewed, colName);
        } else if (colName.equals(stored)) {
            validColumns.put(HiveTableAttr.stored, colName);
        } else if (colName.equals(rowFormat)) {
            validColumns.put(HiveTableAttr.rowFormat, colName);
        } else if (colName.equals(location)) {
            validColumns.put(HiveTableAttr.location, colName);
        } else if (colName.equals(tblproperties)) {
            validColumns.put(HiveTableAttr.tblproperties, colName);
        }

    }
}
