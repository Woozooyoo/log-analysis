
/opt/module/flume-1.7.0/bin/flume-ng agent --conf /opt/module/flume-1.7.0/conf/ -f /opt/module/flume-1.7.0/job/flume-analysis.conf -n a1

/opt/module/kafka_2.11-0.11.0.2/bin/kafka-console-consumer.sh --bootstrap-server hadoop104:9092 --topic analysis-log-0508

sudo /opt/module/nginx/sbin/nginx
/opt/module/apache-tomcat-7.0.72_02/bin/startup.sh

* * * * * source /etc/profile; /opt/module/shell/log-analysis-hdfs2hive.sh

*/10 * * * * /usr/sbin/ntpdate hadoop102

#!/bin/bash
# 获取三分钟之前的时间
systime=`date -d "-3 minute" +%Y%m-%d-%H%M`
# 获取年月
ym=`echo ${systime} | awk -F '-' '{print $1}'`
# 获取日
day=`echo ${systime} | awk -F '-' '{print $2}'`
# 获取小时分钟
hm=`echo ${systime} | awk -F '-' '{print $3}'`

# 执行hive命令
/opt/module/hive-1.7/bin/hive -e "load data inpath '/user/atguigu/log-analysis/${ym}/${day}/${hm}' into table loganalysisdb.ext_startup_logs partition(ym='${ym}',day='${day}',hm='${hm}')"


CREATE external TABLE ext_startup_logs(
    userId string,
    appPlatform string,
    appId string,
    startTimeInMs bigint,
    activeTimeInMs bigint,
    appVersion string,
    city string)
PARTITIONED BY (ym string, day string,hm string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE;