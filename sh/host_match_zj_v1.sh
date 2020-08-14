#!/usr/bin/env bash
#/bin/sh
baseDirForScriptSelf=$(cd `dirname $0`; pwd)
logDay=`date +"%Y%m%d"`
logPath='root/hgq/logs'
root_path="/root/hgq"
in_path="/user/hive/warehouse/db_daily.db/test_orc"
out_path="/user/hgq/zj/out"
result_path="/user/hgq/zj/result"


dpi_hosts=$root_path/hosts.txt
source $root_path/configuration.txt

for host in $(cat ${dpi_hosts})
do
  hosts=${host}","${hosts}
done

for date in `cat $root_path/match_date.txt`
do
    echo '=============================== run by day ======================================='
    echo 'intput_path ==>'$in_path
    echo 'output_path ==>'$out_path
    echo '=============================== run by day ========================================'
    hadoop jar ${baseDirForScriptSelf}/hostmatch-1.0.1-shaded.jar  com.spd.mr.zj.HostZj \
    $in_path \
    $out_path/$date \
    ${hosts} \
    2>&1 |tee -a ${logPath}"/spd_"$logDay".log"


    echo '=============================== fillter ======================================='
    echo 'intput_path ==>'$out_path/$date
    echo 'output_path ==>'$result_path/$date
    echo 'result_path ==>'$result_path/_FILTER_DATE_
    echo 'fillter_day ==>'$fillter_day
    echo '=============================== fillter ========================================'
    hadoop jar ${baseDirForScriptSelf}/hostmatch-1.0.1-shaded.jar  com.spd.mr.zj.fillter.HostZjFillter \
    $out_path/$date \
    $result_path/$date \
    $result_path/_FILTER_DATE_ \
    $date \
    $fillter_day
    2>&1 |tee -a ${logPath}"/spd_"$logDay".log"

done