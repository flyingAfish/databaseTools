package org.flingAfish.database.hive;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

@Getter
@AllArgsConstructor
@ToString
public class HiveTable {
    private String dbName;
    private String tableName;
    private String tableComment;
    private List<String> colName;
    private List<String> colComment;
    private List<String> dataType;
    private String tableType;
    private TreeMap<Integer, String> primaryKey;
    private List<Boolean> unique;
    private List<Boolean> notNull;
    private List<String> defaultValue;
    private List<String> check;
    private TreeMap<Integer, String> partitioned; // 分区顺序,分区字段
    private TreeMap<Integer, String> clustered;
    private Integer numBuckets;
    private TreeMap<Integer, String> sorted;
    private String skewed;
    private String stored;
    private String rowFormat;
    private String location;
    private String tblproperties;

    HiveTable() {
        colName = new ArrayList<>();
        colComment = new ArrayList<>();
        dataType = new ArrayList<>();
        primaryKey = new TreeMap<>();
        unique = new ArrayList<>();
        notNull = new ArrayList<>();
        defaultValue = new ArrayList<>();
        check = new ArrayList<>();
        partitioned = new TreeMap<>();
        clustered = new TreeMap<>();
        sorted = new TreeMap<>();
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setTableComment(String tableComment) {
        this.tableComment = tableComment;
    }

    public void setTableType(String tableType) {
        this.tableType = tableType;
    }

    public void setNumBuckets(Integer numBuckets) {
        this.numBuckets = numBuckets;
    }

    public void setSkewed(String skewed) {
        this.skewed = skewed;
    }

    public void setRowFormat(String rowFormat) {
        this.rowFormat = rowFormat;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public void setTblproperties(String tblproperties) {
        this.tblproperties = tblproperties;
    }

    public void setOrDefaultDbName(String dbName, String defaultValue) {
        this.dbName = dbName == null ? defaultValue : dbName;
    }

    public void setOrDefaultTableComment(String tableComment, String defaultValue) {
        this.tableComment = tableComment == null ? defaultValue : tableComment;
    }

    public void setOrDefaultStored(String stored, String defaultValue) {
        this.stored = stored == null ? defaultValue : stored;
    }

    public void setOrDefaultRowFormat(String rowFormat, String defaultValue) {
        this.rowFormat = rowFormat == null ? defaultValue : rowFormat;
    }

    public void setOrDefaultLocation(String location, String defaultValue) {
        this.location = location == null ? defaultValue : location;
    }

    public void setOrDefaultTblproperties(String tblproperties, String defaultValue) {
        this.tblproperties = tblproperties == null ? defaultValue : tblproperties;
    }

    public void setOrDefaultNumBuckets(Integer numBuckets, Integer defaultValue) {
        this.numBuckets = numBuckets == null ? defaultValue : numBuckets;
    }

    public void clear() {
        colName.clear();
        colComment.clear();
        dataType.clear();
        primaryKey.clear();
        unique.clear();
        notNull.clear();
        defaultValue.clear();
        check.clear();
        partitioned.clear();
        clustered.clear();
        sorted.clear();
    }
    
}
