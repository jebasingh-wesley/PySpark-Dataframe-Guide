# PySpark-Dataframe-Guide
PySpark Dataframe Complete Guide (with COVID-19 Dataset) Spark which is one of the most used tools when it comes to working with Big Data.  While once upon a time Spark used to be heavily reliant on RDD manipulations, Spark has now provided a DataFrame API for us Data Scientists to work with.




In this notebook, We will learn standard Spark functionalities needed to work with DataFrames, and finally some tips to handle the inevitable errors you will face.

I'm going to skip the Spark Installation part for the sake of the notebook, so please go to Apache Spark Website to install Spark that are right to your work setting.

It looks ok right now, but sometimes as we the number of columns increases, the formatting becomes not too great. I have noticed that the following trick helps in displaying in pandas format in my Jupyter Notebook.

The .toPandas() function converts a Spark Dataframe into a Pandas Dataframe, which is much easier to play with.

[3] Change Column Names
We can select a subset of columns using the select
[4] Sort by Column
[5] Change Column Type
[6] Filter
We can filter a data frame using multiple conditions using AND(&), OR(|) and NOT(~) conditions. For example, we may want to find out all the different infection_case in Daegu with more than 10 confirmed cases.
[7] GroupBy
[8] Joins
Here, We will go with the region file which contains region information such as elementary_school_count, elderly_population_ratio, etc.



2. Use SQL with DataFrames
We first register the cases dataframe to a temporary table cases_table on which we can run SQL operations. As you can see, the result of the SQL select statement is again a Spark Dataframe.

All complex SQL queries like GROUP BY, HAVING, AND ORDER BY clauses can be applied in 'Sql' function




3. Create New Columns
There are many ways that you can use to create a column in a PySpark Dataframe.

[1] Using Spark Native Functions
We can use .withcolumn along with PySpark SQL functions to create a new column. In essence, you can find String functions, Date functions, and Math functions already implemented using Spark functions. Our first function, the F.col function gives us access to the column. So if we wanted to add 100 to a column, we could use F.col as:



[2] Using Spark UDFs
Sometimes we want to do complicated things to a column or multiple columns. This could be thought of as a map operation on a PySpark Dataframe to a single column or multiple columns. While Spark SQL functions do solve many use cases when it comes to column creation, I use Spark UDF whenever I need more matured Python functionality. \

To use Spark UDFs, we need to use the F.udf function to convert a regular python function to a Spark UDF. We also need to specify the return type of the function. In this example the return type is StringType()
