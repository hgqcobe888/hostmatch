#!/usr/bin/env bash
#/bin/sh
baseDirForScriptSelf=$(cd `dirname $0`; pwd)
logDay=`date +"%Y%m%d"`
logPath='root/hgq/logs'
root_path="/root/hgq"
in_path="/user/hgq/zj/orgdata"
out_path="/user/hgq/zj/out"

dpi_hosts=$root_path/hosts.txt
source $root_path/configuration.txt

for host in $(cat ${dpi_hosts})
do
  hosts=${host}","${hosts}
done

for dat in `cat $root_path/match_date.txt`
do
    hadoop jar ${baseDirForScriptSelf}/hostmatch-1.0.1-shaded.jar  com.spd.mr.ORCSample6 \
    -D mapreduce.job.inputformat.class=org.apache.orc.mapreduce.OrcInputFormat \
    $spd_split \
    $in_path/day=$date  \
    $out_path/$date \
    ${hosts}
done