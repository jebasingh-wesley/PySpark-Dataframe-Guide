import pandas as pd
import numpy as np
from datetime import date, timedelta, datetime
import time

import pyspark # only run this after findspark.init()
from pyspark.sql import SparkSession, SQLContext
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SparkFiles

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType

#data source
case_csv = 'https://raw.githubusercontent.com/hyunjoonbok/PySpark/master/data/Case.csv'
regions_csv = 'https://raw.githubusercontent.com/hyunjoonbok/PySpark/master/data/Region.csv'
# data = pd.read_csv(url,sep=",") # use sep="," for coma separation.

# Initiate the Spark Session
spark = SparkSession.builder.appName('covid-example').getOrCreate()

#pandas read_csv
cases = pd.read_csv(case_csv)

#converting pandas to spark
sparkDF=spark.createDataFrame(cases)
# sparkDF.printSchema()
# sparkDF.show(10)

#conver spark to pandas
sparkDF.limit(10).toPandas()

"Change Column Names using pandas"
#To change a single column
cases = sparkDF.withColumnRenamed("infection_case","infection_source")

#To change all columns
cases = cases.toDF(*['case_id', 'province', 'city', 'group', 'infection_case', 'confirmed','latitude', 'longitude'])

"We can select a subset of columns using the select"
cases = cases.select('province','city','infection_case','confirmed')

"Sort by Column"
cases.sort("confirmed")

# Descending Sort
cases.sort(F.desc("confirmed"))

#Change Column Type
cases = cases.withColumn('confirmed', F.col('confirmed').cast(IntegerType()))
cases = cases.withColumn('city', F.col('city').cast(StringType()))

"Filter"
cases.filter((cases.confirmed>10) & (cases.province=='Daegu'))

"GroupBy"
cases.groupBy(["province","city"]).agg(F.sum("confirmed") ,F.max("confirmed"))



"Joins"
"Here, We will go with the region file which contains region information such as elementary_school_count, elderly_population_ratio, etc."


"pandas read_csv"
regions = pd.read_csv(regions_csv)

"converting pandas to spark"
regions=spark.createDataFrame(regions)
# sparkDF.show(10)

"conver spark to pandas"
regions.limit(10).toPandas()

# Left Join 'Case' with 'Region' on Province and City column
cases = cases.join(regions, ['province','city'],how='left')







