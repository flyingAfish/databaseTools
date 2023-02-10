# databaseTools
## 1.简介
  这是一个数据库管理工具，可以根据用户编辑的excel数据模型生成建表语句，后续将支持查询数据库
内建函数，数据库增删改查语句格式，建表建库等DDL语句格式。

## 2.支持数据库
hive(目前支持)
mysql
oracle
db2

## 3.支持系统
linux

## 4.安装
+ 下载databaseTools.zip包
+ 进入linux系统，解压后cd databaseTools/完成安装

## 5.使用
+ 进入配置目录cd databaseTools/config/
+ 下载hive数据模型模板.xlsx到本地
+ 根据模板内说明填写模型文档后放入databaseTools/config/目录下
+ 用户的模型文档可以重新命名，但是必须在config目录下，且不能有子目录

### 5.1.操作
```shell
[root@hadoop01 bin]# cd databaseTools/bin/
[root@hadoop01 bin]# sh databaseTools.sh hive数据模型模板.xlsx ./hiveCreateTable.sql
[root@hadoop01 bin]# ll
total 8
-rwxr-xr-x 1 root root 1185 Feb  9 23:19 databaseTools.sh
-rw-r--r-- 1 root root 2192 Feb 10 21:53 hiveCreateTable.sql

```

### 5.2.查看结果
```sql
[root@hadoop01 bin]# cat hiveCreateTable.sql 
-- dim_fund_detail_content_info_di
-- dim_fund_detail_media_info_df

CREATE TABLE IF NOT EXISTS dim_fund.dim_fund_detail_content_info_di(
    `content_id` string DEFAULT null COMMENT '内容ID'
   ,`content_title` string DEFAULT null COMMENT '标题'
   ,`content_type` string DEFAULT null COMMENT '内容类型'
   ,`publish_time` string DEFAULT null COMMENT '基金展示发布时间'
   ,`update_time` string DEFAULT null COMMENT '基金更新时间'
   ,`is_valid` string DEFAULT null COMMENT '是否上线基金'
   ,`video_length` double DEFAULT null COMMENT '基金内容视频时⻓'
   ,`audio_length` double DEFAULT null COMMENT '基金内容⾳频时⻓'
   ,`live_length` double DEFAULT null COMMENT '基金内容直播回看时⻓'
   ,`is_media_account` string DEFAULT null COMMENT '是否来⾃基金号'
   ,`media_account_id` string DEFAULT null COMMENT '账号ID'
   ,`etl_dt` string DEFAULT null COMMENT '跑批日期'
) COMMENT '基金信息采集数据'
PARTITIONED BY (bus_dt string COMMENT '分区字段')
CLUSTERED BY (content_id) SORTED BY (publish_time ASC, update_time DESC) INTO 5 BUCKETS
ROW FORMAT DELIMITED
   FIELDS TERMINATED BY '\001'
   COLLECTION ITEMS TERMINATED BY '\002'
   MAP KEYS TERMINATED BY '\003'
STORED AS ORC
LOCATION '/hive/app/dim_fund_detail_content_info_di';

CREATE TABLE IF NOT EXISTS dim_fund.dim_fund_detail_media_info_df(
    `media_account_id` string DEFAULT null COMMENT '基金号账号ID'
   ,`account_name` string DEFAULT null COMMENT '账号名'
   ,`account_type` string DEFAULT null COMMENT '账号类型'
   ,`account_status` string DEFAULT null COMMENT '账号状态'
   ,`update_time` string DEFAULT null COMMENT '更新时间'
   ,`etl_dt` string DEFAULT null COMMENT '跑批日期'
) COMMENT '基金号账号采集数据'
PARTITIONED BY(create_time string COMMENT '创建时间', bus_dt string COMMENT '分区字段')
CLUSTERED BY (media_account_id, account_name) SORTED BY (create_time ASC, update_time DESC) INTO 5 BUCKETS
ROW FORMAT DELIMITED
   FIELDS TERMINATED BY '\001'
   COLLECTION ITEMS TERMINATED BY '\002'
   MAP KEYS TERMINATED BY '\003'
STORED AS PARQUET
LOCATION '/hive/app/dim_fund_detail_media_info_df';

```