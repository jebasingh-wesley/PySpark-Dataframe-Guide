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
from pyspark.sql.types import IntegerType, StringType, DoubleType, BooleanType
from pyspark.sql.types import StructType, StructField

from PySpark_Dataframe import cases

spark = SparkSession.builder.appName('covid-example-sql').getOrCreate()

"All complex SQL queries like GROUP BY, HAVING, AND ORDER BY clauses can be applied in 'Sql' function"

cases.registerTempTable('cases_table')
newDF = spark.sql('select confirmed from cases_table where confirmed > 100')

"Using Spark Native Functions"
#Create New Columns

"We can use .withcolumn along with PySpark SQL functions to create a new column. In essence, you can find String functions, Date functions, "
"and Math functions already implemented using Spark functions. Our first function, the F.col function gives us access to the column. "
"So if we wanted to add 100 to a column, we could use F.col as:"

casesWithNewConfirmed = cases.withColumn("NewConfirmed", 100 + F.col("confirmed"))

#We can also use math functions like F.exp function:

casesWithExpConfirmed = cases.withColumn("ExpConfirmed", F.exp("confirmed"))

"Using Spark UDFs"

# To use Spark UDFs, we need to use the F.udf function to convert a regular python function to a Spark UDF.
# We also need to specify the return type of the function.
# In this example the return type is StringType()

def casesHighLow(confirmed):
    if confirmed < 50:
        return 'low'
    else:
        return 'high'


# convert to a UDF Function by passing in the function and return type of function
casesHighLowUDF = F.udf(casesHighLow, StringType())
CasesWithHighLow = cases.withColumn("HighLow", casesHighLowUDF("confirmed"))
# CasesWithHighLow.show(10)

"Spark Window Functions"

timeprovinc = "https://raw.githubusercontent.com/hyunjoonbok/PySpark/master/data/TimeProvince.csv"
timeprovince = pd.read_csv(timeprovinc)
timeprovince=spark.createDataFrame(timeprovince)
timeprovince.limit(10).toPandas()


from pyspark.sql.window import Window

"For example, you may want to have a column in your cases table that provides the rank of infection_case based on the number of infection_case in a province. We can do this by"

windowSpec = Window().partitionBy(['province']).orderBy(F.desc('confirmed'))
cases.withColumn("rank",F.rank().over(windowSpec))

"Lag Variables"
# For example, a model might have variables like the price last week or sales quantity the previous day. We can create such features using the lag function with window functions.


from pyspark.sql.window import Window

windowSpec = Window().partitionBy(['province']).orderBy('date')
timeprovinceWithLag = timeprovince.withColumn("lag_7",F.lag("confirmed", 7).over(windowSpec))
timeprovinceWithLag.filter(timeprovinceWithLag.date>'2020-03-10')

"Rolling_Aggregations"

"For example, we might want to have a rolling 7-day sales sum/mean as a feature for our sales regression model. " \
"Let us calculate the rolling mean of confirmed cases for the last 7 days here. " \
"This is what a lot of the people are already doing with this dataset to see the real trends."

# If we had used rowsBetween(-7,-1), we would just have looked at past 7 days of data and not the current_day
windowSpec = Window().partitionBy(['province']).orderBy('date').rowsBetween(-6,0)
timeprovinceWithRoll = timeprovince.withColumn("roll_7_confirmed",F.mean("confirmed").over(windowSpec))
timeprovinceWithRoll.filter(timeprovinceWithLag.date>'2020-03-10')


"Pivot DataFrames"


pivotedTimeprovince = timeprovince.groupBy('date').pivot('province') \
.agg(F.sum('confirmed').alias('confirmed') , F.sum('released').alias('released'))
pivotedTimeprovince.limit(10).toPandas()

"Caching"

# spark.cache().count()

"Save and Load from an intermediate step"

# df.write.parquet("data/df.parquet")
# df.unpersist()
# spark.read.load("data/df.parquet")
