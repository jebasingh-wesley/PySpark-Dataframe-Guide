# PySpark DataFrame Guide

## PySpark DataFrame Complete Guide (with COVID-19 Dataset)

Apache Spark is one of the most widely used tools for working with Big Data. While Spark initially relied heavily on RDD manipulations, it now provides a DataFrame API that is more user-friendly for Data Scientists.

### Overview

In this notebook, we will explore standard Spark functionalities needed to work with DataFrames, and provide tips to handle common errors.

Note: The Spark installation process is skipped in this notebook. Please refer to the [Apache Spark Website](https://spark.apache.org/downloads.html) to install Spark according to your work setting.

### Displaying DataFrames

When the number of columns increases, formatting can become an issue. The `.toPandas()` function converts a Spark DataFrame into a Pandas DataFrame, which is much easier to work with in a Jupyter Notebook.

### Table of Contents
1. [Change Column Names](#change-column-names)
2. [Select Subset of Columns](#select-subset-of-columns)
3. [Sort by Column](#sort-by-column)
4. [Change Column Type](#change-column-type)
5. [Filter DataFrames](#filter-dataframes)
6. [GroupBy Operations](#groupby-operations)
7. [Joins](#joins)
8. [Using SQL with DataFrames](#using-sql-with-dataframes)
9. [Create New Columns](#create-new-columns)

### 1. Change Column Names
You can select a subset of columns using the `select` method.

### 2. Select Subset of Columns
You can select specific columns from a DataFrame using the `select` method.

### 3. Sort by Column
Sort the DataFrame by a specific column.

### 4. Change Column Type
You can change the data type of a column in a DataFrame.

### 5. Filter DataFrames
Filter a DataFrame using multiple conditions with `AND` (&), `OR` (|), and `NOT` (~) conditions. For example, finding all different infection cases in Daegu with more than 10 confirmed cases.

### 6. GroupBy Operations
Group data by specific columns to perform aggregate operations.

### 7. Joins
Perform joins on DataFrames. Here, we will use a region file containing information such as elementary school count, elderly population ratio, etc.

### 8. Using SQL with DataFrames
Register a DataFrame as a temporary table to run SQL operations. The result of the SQL `SELECT` statement is a Spark DataFrame. All complex SQL queries like `GROUP BY`, `HAVING`, and `ORDER BY` clauses can be applied using the `sql` function.

### 9. Create New Columns
There are many ways to create a column in a PySpark DataFrame.

#### 9.1 Using Spark Native Functions
Use `.withColumn` along with PySpark SQL functions to create a new column. Spark functions include string functions, date functions, and math functions. For example, the `F.col` function gives access to the column. To add 100 to a column, use `F.col`.

#### 9.2 Using Spark UDFs
For more complex operations on columns, use Spark UDFs. A UDF (User Defined Function) can be thought of as a map operation on a single column or multiple columns. To use Spark UDFs, convert a regular Python function to a Spark UDF using the `F.udf` function and specify the return type (e.g., `StringType`).

---

Feel free to explore the code, contribute, and raise issues if you encounter any. Happy coding!
