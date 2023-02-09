#!/bin/bash

rootDir=$(cd `dirname $0`/..;pwd)

excelModel=$1
exportFile=$2

# ===========================================参数定义===========================================
java_exce=$JAVA_HOME/bin/java
main_jar=databaseTools-1.0-SNAPSHOT.jar
import_file=${rootDir}/config/${excelModel}

# ===========================================函数区===========================================
function func_help(){
    echo "----------------------------------
帮助信息
请数据两个参数：
    excelModel -- config目录中的数据模型模板
    exportFile -- 输出建表语句文件位置
Usage: sh ${rootDir}/bin/$0 hive数据模型模板.xlsx ./hiveCreateTable.sql
----------------------------------"
    exit 1
}

function func_check_file_exists(){
    __file=$1
    if [ -f "${__file}" ]
    then
        echo ""
    else
        echo "${__file}文件不存在！"
        func_help
    fi
 }

# ===========================================主程序===========================================
if [ $# -ne 2 ]
then
    func_help
fi

func_check_file_exists "${import_file}"

$java_exce -jar ${rootDir}/lib/${main_jar} --excelModel ${import_file} --exportFile ${exportFile}
