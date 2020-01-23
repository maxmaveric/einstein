'''
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
