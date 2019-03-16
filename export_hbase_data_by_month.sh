#!/bin/bash
#coding=utf-8

############################################################################################################
###  脚本同级目录有log和data文件夹
###  author: dongming
###	 date:   2018-09-18
###  功能：(只适用于按月存储的表)
###    1、按月同步hbase的数据，可以重复执行
###    2、支持同步到本地
###      2.1、日志文件当前目录的log文件夹下
###      2.2、支持断点续传，以region文件夹为最小单位
###    3、同步到另一个hadoop集群
###      3.1、日志文件在hdfs的/var/log/exortData/文件夹下
###      3.2、不支持断点续传，重新传要清空目录下原来已经同步过来的数据
###
###  命令：
###    [Usage]: export_hbase_data_by_month.sh tableName startMonth endMonth [localPath|hdfsPath];
###  
###  注意：人脸、视频的region时间年月是完成的格式201801，卡口的时间年月是缩写的1801
###  
###  人脸、视频 
###    [Use Like]: export_hbase_data_by_month.sh SNAP_IMAGE_INFO 201801 201802 /mnt/disk1/data_bak
###  卡口
###    [Use Like]: export_hbase_data_by_month.sh BAYONET_VEHICLEPASS 1801 1802 /mnt/disk1/data_bak
##############################################################################################################

#init log file 
HOME=`dirname "$0"`
HOME=`cd "$HOME">/dev/null; pwd`
LOG_FILE="$HOME/log/export_hbase_data.`date +%Y-%m-%d`.log"
RESULT_FILE="$HOME/result.txt"

mkdir -p "$HOME/log/"
mkdir -p "$HOME/data/"
touch $LOG_FILE

batchSize=1

function write_log(){
	echo "`date +%Y-%m-%d\ %H:%M:%S` $*"
	echo "`date +%Y-%m-%d\ %H:%M:%S` $*" >> $LOG_FILE 2>&1
}
#write_log $#


#check param
if [[ $# -lt 3 || $# -gt 4 ]]; then
	echo "[Usage   ]:export_hbase_data_by_month.sh tableName startMonth endMonth [localPath|hdfsPath] ";
	echo "[Use Like]:export_hbase_data_by_month.sh SNAP_IMAGE_INFO 201801 201802 /mnt/disk1/data";
	echo "[Use Like]:export_hbase_data_by_month.sh BAYONET_VEHICLEPASS 1801 1802 /mnt/disk1/data";
	exit 0 ; 
fi
tableName=$1
startMonth=$2
endMonth=$3

dateLen=${#startMonth}

#default
localPath="$HOME/data" 

CPCMD="fs -copyToLocal"
#default is local cp
cpFlag=1 

if [[ $# = 4 ]]; then
	localPath=$4
	fileFlag=`echo ${localPath:0:7}`
	if [[ "x"$fileFlag = "xhdfs://" ]];then
		CPCMD="distcp"
		#hdfs cp to hdfs
		cpFlag=0;
	fi
fi

cm_clusters=/usr/lib/cloudmanager/components/lark/data/clusters
CLUSTER_NUM=`ls $cm_clusters | wc -l`
CLUSTER_NAME="LOCALCLUSTER"
if [ $CLUSTER_NUM -gt 1 ]; then
	CLUSTER_NAME=`ls $cm_clusters/ | grep -v LOCALCLUSTER`
fi
write_log "CLUSTER_NAME=[$CLUSTER_NAME]"
cat $cm_clusters/$CLUSTER_NAME/cluster.yml | grep -v -E "\- id|\-[0-9]+\"$" | grep id | grep -E "SERVICE-.*-.*" | sed s/[[:space:]]//g |awk -F "\"" '{print $2}' | uniq > $HOME/SERVICE_NAME
HADOOP_ID=`cat $HOME/SERVICE_NAME|grep SERVICE-HADOOP-`
HBASE_ID=`cat $HOME/SERVICE_NAME|grep SERVICE-HBASE-`
HADOOP_HOME="/usr/lib/$CLUSTER_NAME/$HADOOP_ID"
HBASE_HOME="/usr/lib/$CLUSTER_NAME/$HBASE_ID"
write_log "HADOOP_HOME=[$HADOOP_HOME];HBASE_HOME=[$HBASE_HOME]"
#获取region的相关信息
declare scan_meta="scan 'hbase:meta', {COLUMNS => 'info:regioninfo'}"
echo $scan_meta | $HBASE_HOME/bin/hbase shell |grep $tableName |grep "NAME" |awk -F "[ ,.]" '{if(length($2)>0){if(length($3)>0){ny=$2"_"substr($3,0,'"$dateLen"')}else{ny=$2"_000000"};print $5,ny}}'> $HOME/scan1.txt
#过滤出指定范围内的region
awk -F "[ _]" '{if($NF>='"$startMonth"' && $NF<='"$endMonth"'){print $1,$NF}}' scan1.txt >scan3.txt
mkdir $localPath/$tableName
$HADOOP_HOME/bin/hadoop $CPCMD /hbase/data/default/$tableName/.tabledesc $localPath/$tableName

####################################################################
hdfsPathPrefix="/hbase/data/default/$tableName"
lastOper=0;
if [[ "$cpFlag" != "0" ]];then
	#=========================================
	#hdfs cp to local
	#=========================================
	if [[ -s "$RESULT_FILE" ]];then
		#结果文件已经存在，询问用户是否继续上次操作？还是重新备份数据？
		echo "---------------------------------------------------------------------------------------------"
		echo -n "File [$RESULT_FILE] already exists. Do you want to continue with last operation? y|n|c ? : "
		read input
		case $input in
		y|Y)
			write_log "Resume from break-point"
			#删除临时文件
			rm -fr $HOME/result_tmp.txt
			#拷贝状态结果文件
			cp -f $HOME/result.txt $HOME/result_tmp.txt
			#循环遍历数据
			while read line
			do
				declare line_tmp=$(echo $line | awk -F ' ' '{print $1}')
				declare line_stats=$(echo $line | awk -F ' ' '{print $2}')
				write_log "Region Folder = [$line_tmp]; Status = [$line_stats]"
				if [[ -n "$line_tmp" && $line_stats != 0 ]]; then
					hdfsPath="$hdfsPathPrefix/$line_tmp"
					write_log "$HADOOP_HOME/bin/hadoop $CPCMD $hdfsPath $localPath/$tableName"
					$HADOOP_HOME/bin/hadoop $CPCMD $hdfsPath $localPath/$tableName
					#更新状态文件
					if [ "x$line_stats" != "x$?" ];then
						line_stats=$?
						awk -v line_tmp=$line_tmp '{if ($1==line_tmp) $2='"${line_stats}"'}1' result_tmp.txt 1<> result_tmp.txt
					fi
				fi
				#递增行数
				((lastOper++));
			done < $HOME/result.txt
			
			sleep 1s;
			mv -f $HOME/result_tmp.txt $HOME/result.txt
			;;
		n|N)
			write_log "Empty the result file and restart the operation."
			cat /dev/null > $RESULT_FILE;
			lastOper=0;
			;;
		*)
			write_log "Cancel This Operation."
			exit 0;
		   ;;
		esac
	fi

	((lastOper++));

	write_log "sed -n '"$lastOper",\$p' $HOME/scan3.txt >$HOME/scan4.txt"
	sed -n ''"$lastOper"',$p' $HOME/scan3.txt >$HOME/scan4.txt

	#循环拉取数据
	while read line
	do
		declare line_tmp=$(echo $line | awk -F ' ' '{print $1}')
		if [ -n "$line_tmp" ]; then
			#开始时间
			start_time=`date +%s`
			hdfsPath="$hdfsPathPrefix/$line_tmp"
			write_log "$HADOOP_HOME/bin/hadoop $CPCMD $hdfsPath $localPath/$tableName"
			$HADOOP_HOME/bin/hadoop $CPCMD $hdfsPath $localPath/$tableName
			#增加耗时
			echo "$line_tmp $? $((`date +%s`-$start_time))s" >> result.txt
		fi
	done < $HOME/scan4.txt
else
	#=========================================
	#hdfs cp to hdfs
	#=========================================
	#生成需要拷贝的文件列表
	write_log "awk -F ' ' '{print "'"$hdfsPathPrefix/"'"$1 }' scan3.txt >$HOME/scan4.txt"
	awk -F ' ' '{print "'"$hdfsPathPrefix/"'"$1 }' scan3.txt >$HOME/scan4.txt
	tmpPath="/tmp/exportData"
	$HADOOP_HOME/bin/hadoop fs -test -d $tmpPath
	if [ $? = 1 ];then
		$HADOOP_HOME/bin/hadoop fs -mkdir $tmpPath
	fi
	$HADOOP_HOME/bin/hadoop fs -test -e $tmpPath/scan4.txt
	if [ $? = 1 ];then
		$HADOOP_HOME/bin/hadoop fs -rm -r $tmpPath/scan4.txt
	fi
	$HADOOP_HOME/bin/hadoop fs -put $HOME/scan4.txt $tmpPath/scan4.txt
	
	#集群与集群拷贝
	write_log "$HADOOP_HOME/bin/hadoop distcp -log /var/log/exportData/ -f $HOME/scan4.txt $localPath"
	write_log "Log file directory [hdfs file system /var/log/exportData/],Please go to the file directory to view the details of the task."
	$HADOOP_HOME/bin/hadoop distcp -log /var/log/exportData/ -f $tmpPath/scan4.txt $localPath
fi
