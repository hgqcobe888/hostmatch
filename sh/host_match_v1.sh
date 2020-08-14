#!/usr/bin/env bash
#/bin/sh
baseDirForScriptSelf=$(cd `dirname $0`; pwd)
logDay=`date +"%Y%m%d"`
logPath='/app/zmcc_use1/logs'
root_path="/app/zmcc_use1/test"
in_path="/ns2/jc_zz_bdcpt/bdcpt_ns2_hive_db/http_zj_2020"
out_path="/tmp"

dpi_hosts=$root_path/hosts.txt
source $root_path/configuration.txt

for host in $(cat ${dpi_hosts})
do
  hosts=${host}","${hosts}
done

for date in `cat $root_path/match_date.txt`
do
    hadoop jar ${baseDirForScriptSelf}/hostmatch-1.0.1.jar  com.spd.mr.Host $spd_split $in_path  $out_path/$date ${hosts} 2>&1 |tee -a ${logPath}"/spd_"$logDay".log"
done