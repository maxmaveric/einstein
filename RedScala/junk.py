
for (( i=30; i <= 40; ++i ))
do
start=`date --date="$i days ago" +%Y%m%d`
echo $start
    YEAR=`echo ${start:0:4}`
    MONTH=`echo ${start:4:2}`
    DAY=`echo ${start:6:2}`

hdfs dfs -rm -r /data/rt/raw/public/pricing_integrity/raw_partitioned/sqoop/adr4/md012p/year=$YEAR/month=$MONTH/day=$DAY
hdfs dfs -rm -r /data/rt/raw/public/pricing_integrity/raw_partitioned/sqoop/adr4/md013p/year=$YEAR/month=$MONTH/day=$DAY
hdfs dfs -rm -r /data/rt/raw/public/pricing_integrity/raw_partitioned/sqoop/adr4/md018p/year=$YEAR/month=$MONTH/day=$DAY
hdfs dfs -rm -r /data/rt/raw/public/pricing_integrity/raw_partitioned/sqoop/adr4/md019p/year=$YEAR/month=$MONTH/day=$DAY
hdfs dfs -rm -r /data/rt/raw/public/pricing_integrity/raw_partitioned/sqoop/adr4/md495p/year=$YEAR/month=$MONTH/day=$DAY
hdfs dfs -rm -r /data/rt/raw/public/pricing_integrity/raw_partitioned/sqoop/adr4/mn003p/year=$YEAR/month=$MONTH/day=$DAY
done

exit 0




source ../config/shell.config
$KINIT_COMMAND
CURDTTM=`date +%Y%m%d%H%M%S`
LOG_FILE_PREFIX=`echo $0 | awk -F'.' '{print $1}'`
LOGFILE=$LOG_DIR/seq000A02_maintenance'_'$CURDTTM.log
echo "LOG_FILE : "$LOGFILE



for (( i=30; i <= 40; ++i ))
do
start=`date --date="$i days ago" +%Y%m%d`
echo $start
    YEAR=`echo ${start:0:4}`
    MONTH=`echo ${start:4:2}`
    DAY=`echo ${start:6:2}`
BEELINE_URL="beeline -u 'jdbc:hive2://pla-w02hdp07.walgreens.com:2181,pla-w02hdp08.walgreens.com:2181,pla-w02hdp09.walgreens.com:2181,pla-w02hdp16.walgreens.com:2181,pla-w02hdp18.walgreens.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2;principal=hive/_HOST@PROD.HADOOP.WALGREENS.COM;transportMode=binary;httpPath=cliservice' --showHeader=false --outputformat=csv2 --verbose true --hivevar TODAY=$(date +"%Y-%m-%d") --hivevar MAX_CYLG_DATETIME=$MAX_CYLG_DATETIME --hivevar YEAR='$YEAR' --hivevar MONTH='$MONTH' --hivevar DAY='$DAY'"
echo $BEELINE_URL
$BEELINE_URL -i $HIVE_CONFIG_FILE -f $HIVE_DIR/maintenance/seq000A02_blue_uranium_maintenance_hive.sql >> $LOGFILE 2>>$LOGFILE

done




BEELINE_URL="beeline -u 'jdbc:hive2://pla-w02hdp07.walgreens.com:2181,pla-w02hdp08.walgreens.com:2181,pla-w02hdp09.walgreens.com:2181,pla-w02hdp16.walgreens.com:2181,pla-w02hdp18.walgreens.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2;principal=hive/_HOST@PROD.HADOOP.WALGREENS.COM;transportMode=binary;httpPath=cliservice' --showHeader=false --outputformat=csv2 --verbose true --hivevar TODAY=$(date +"%Y-%m-%d") --hivevar MAX_CYLG_DATETIME=$MAX_CYLG_DATETIME --hivevar YEAR='$YEAR' --hivevar MONTH='$MONTH' --hivevar DAY='$DAY'"





sqoop import -Dmapred.job.queue.name=$JOB_QUEUE -Dmapreduce.map.output.compress=false -Dmapreduce.output.fileoutputformat.compress=false -Doraoop.timestamp.string=false -Dorg.apache.sqoop.splitter.allow_text_splitter=true \
--connect jdbc:oracle:thin:@$PCMS_SERVER:1521/$PCMS_SID \
--username $PCMS_UID \
--password-file /user/$USER_ID/.pcms-file \
--query """SELECT A.*
FROM (SELECT PB.PRIB_CODE AS STORE,
             ) A WHERE \$CONDITIONS""" \
--split-by STORE \
--fields-terminated-by '\001' \
--lines-terminated-by '\n' \
--delete-target-dir --target-dir $HDFS_ROOT_DIR_PATH_RAW_TMP/blur_pcms_pricing_temp \
-m 1 >> $LOGFILE 2>>$LOGFILE






while read line; do    
    echo $line
        STR_NBR=`echo $line | awk -F'_' '{print $2}' |awk -F'-' '{print $1}'`
        echo "extracting "$STR_NBR
        echo "extracting "$STR_NBR >> $LOGFILE 2>>$LOGFILE
        unzip -o $line
        CURDTTM=`date +%Y%m%d%H%M`
        #mv activeprice.txt     $STR_NBR.$CURDTTM.activeprice.txt
        mv activepromotion.txt $STR_NBR.$CURDTTM.activepromotion.txt
        mv sitelevel.txt       $STR_NBR.$CURDTTM.sitelevel.txt
        mv pbcbatchcount.txt   $STR_NBR.$CURDTTM.pbcbatchcount.txt
        mv priceconstant*.txt  $STR_NBR.$CURDTTM.priceconstant.txt
                
        #mv $STR_NBR.$CURDTTM.refined_active_markdown.txt activemarkdown  
        #mv $STR_NBR.$CURDTTM.activeprice.txt     activeprice              
        mv $STR_NBR.$CURDTTM.activepromotion.txt activepromotion
        mv $STR_NBR.$CURDTTM.sitelevel.txt       sitelevel
        mv $STR_NBR.$CURDTTM.pbcbatchcount.txt   pbcbatchcount
        mv $STR_NBR.$CURDTTM.priceconstant.txt $ROOT_LOCAL/sap_ibarra/$DT_TODAY.priceconstant/
        rm -f *.txt
        rm -f $line
        #sleep 1
done < filelist.fl


DT_TODAY=`date +%Y%m%d`
echo $DT_TODAY




Created on Jul 9, 2019

@author: MAX
'''
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import HiveContext
from pyspark.sql.types import *
from pyspark.sql import Row
from pyspark.sql.functions import *
import datetime
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import *

SET hive.execution.engine = tez;
SET tez.queue.name=${queue_name};
SET mapreduce.job.queuename=${queue_name};
SET mapred.map.tasks.speculative.execution = true;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.auto.convert.join=false;
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reduce.enabled = true;
SET hive.exec.compress.output=false;
SET hive.auto.convert.join=false;
set tez.runtime.shuffle.fetch.buffer.percent=0.9; 
set hive.mapjoin.hybridgrace.hashtable=false;
set hive.tez.container.size=8000;
set hive.tez.java.opts=-server -Djava.net.preferIPv4Stack=true -XX:NewRatio=8 -XX:+UseNUMA -XX:+UseG1GC -XX:+ResizeTLAB -XX:+PrintGCDetails -verbose:gc -XX:+PrintGCTimeStamps -Xmx6550m;
set hive.exec.reducers.bytes.per.reducer=67108864;
set hive.auto.convert.join.noconditionaltask.size=2147483648;
set hive.exec.parallel=true;
set hive.fetch.task.aggr=true;
set hive.optimize.index.filter=true;
set hive.exec.compress.output=false;
set hive.tez.exec.print.summary=false;


pathSapSiteCrossRef = "E://green_mercury//data//sample_data//sample.txt"
transaction_xml_file = "E://green_mercury//data//input/books.xml"

pSalesFileLoc="C://Exercise_Files//cogsley_sales.csv"

sc = SparkContext()
sqlContext = SQLContext(sc)




spark = SparkSession\
        .builder\
        .getOrCreate()




rawSalesDf = spark.read.format("com.databricks.spark.csv").options(header='true', delimiter = ',').load(pSalesFileLoc)

rawSalesDf.show(5)
rawSalesDf.registerTempTable("sample").persist()
filterSaleDF = spark.sql("""
SELECT DISTINCT 
       OrderDate,
       CompanyName,
       Consultant,
       CAST(WageMargin AS DOUBLE) AS WageMargin
FROM sample
WHERE CompanyName LIKE '%Cognizant%' LIMIT 10
""").show(100)




#rawTransactionDF = spark.read.format("com.databricks.spark.xml").options(rowTag ='book').load(transaction_xml_file)
#rawTransactionDF.printSchema()
#rawTransactionDF.show(5)

#df1 = spark.read.format("com.databricks.spark.csv").options(header='true', delimiter = ',').load(pathSapSiteCrossRef)
#df2 = spark.read.format("csv").options(header='true', delimiter = ',').load(pathSapSiteCrossRef)

#df1.selectExpr("EMP_ID").join(df2.select("EMP_ID","NAME"), df1.EMP_ID == df2.EMP_ID).dropDuplicates().show()


#df1.select("EMP_ID").sort("EMP_ID").dropDuplicates().show()
#df1.registerTempTable("TBL001")
#df2.registerTempTable("TBL002")
#df1.printSchema()
#df1.show(5)

#df1.orderBy(df1.EMP_ID.desc()).show(5)
#spark.sql("SELECT A.* FROM TBL001 A LEFT OUTER JOIN TBL002 B ON A.EMP_ID=B.EMP_ID").dropDuplicates().show(10)
