# Databricks notebook source
spark

# COMMAND ----------

# MAGIC %md
# MAGIC File uploaded to /FileStore/tables/sales_data___sales_data-3.csv

# COMMAND ----------

df = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/tables/sales_data___sales_data-3.csv")
df.show()

# COMMAND ----------

spark.sql("drop DATABASE if exists sales_db cascade")
spark.sql("drop table if exists sales")
# dbutils.fs.rm('dbfa:/user/hive/warehouse/sales_db.db/sales',recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC Spark => DataFrame
# MAGIC
# MAGIC Spark Function => to clear
# MAGIC
# MAGIC Spark => SQL(to clean and transform)
# MAGIC
# MAGIC Spark SQL => component pyspark => we can use sql queries ti intract with data
# MAGIC
# MAGIC sql => DataFrame => SQL table store =>Database
# MAGIC
# MAGIC Database => Table <= (already created dataframe)

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS sales_db")
spark.sql("use sales_db")

# COMMAND ----------

spark.sql("""CREATE TABLE IF NOT EXISTS sales(
    OrderId STRING,
    Product STRING,
    QuantityOrdered STRING,
    PriceEach STRING,
    OrderDate STRING,
    PurchaseAddress STRING
)""")

# COMMAND ----------

spark.sql("select * from sales").show()

# COMMAND ----------

# MAGIC %md
# MAGIC view => virtual table
# MAGIC
# MAGIC select => query =>table format

# COMMAND ----------

df.createOrReplaceTempView("temp_sales")
spark.sql("select * from temp_sales").show(5)
# HDD (not physically available => query)

# COMMAND ----------

# view is just for read purpose
# dataframe (not permanently changed)

# COMMAND ----------

# MAGIC %md
# MAGIC second way insert overwrite table select 'col1','col2' from table
# MAGIC
# MAGIC vo table jo humari df se bani (vitrual table ro view table)

# COMMAND ----------

spark.sql(""" INSERT OVERWRITE sales
          SELECT * FROM temp_sales """)

# COMMAND ----------

df.filter(df["Order Id"] != "null").describe().show()

# COMMAND ----------

spark.sql("SELECT * FROM sales").show(5)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales
# MAGIC where OrderId !="Order Id" and OrderId !="null";
# MAGIC
# MAGIC -- kisi dushri mai insert => check
# MAGIC -- select => temporary => select *(check)
# MAGIC -- CTE => common table expression with table (select * from table)

# COMMAND ----------

# MAGIC %sql
# MAGIC with temp_sales as (
# MAGIC   select * from sales
# MAGIC   where OrderId != "Order ID" and orderid != "null"
# MAGIC )
# MAGIC
# MAGIC select * from temp_sales where OrderId = "null";

# COMMAND ----------

# MAGIC %sql
# MAGIC select substr(split(PurchaseAddress,',')[1],2) as city,
# MAGIC   substr(split(PurchaseAddress,',')[2],2,2) as State
# MAGIC   from sales
# MAGIC     where OrderId !="Order ID" and orderid !="null";

# COMMAND ----------

temp_sales_df=spark.sql("""
select cast(OrderId as int) as OrderId,
  product,
  cast(QuantityOrdered as int) QuantityOrdered,
  cast(PriceEach as int) PriceEach,
  to_timestamp(OrderDate,'MM/dd/yy HH:mm') as OrderDate,
  PurchaseAddress,
  substr(split(PurchaseAddress,',')[1],2) as City,
  substr(split(PurchaseAddress,',')[2],2,2) as State,
  year(to_timestamp(OrderDate,'MM/dd/yy HH:mm')) as ReportYear,
  month(to_timestamp(OrderDate, 'MM/dd/yy HH:mm')) as Month
  from sales
  where OrderId !="OrderId" and orderid !="null";
  """)

# COMMAND ----------

temp_sales_df.createOrReplaceTempView('temp_sales') # view => temp_sales

# COMMAND ----------

spark.sql("drop table if exists sales3")
spark.sql("""create table sales3(
        OrderId INT,
        product STRING,
        QuantityOrdered INT,
        priceeach int,
        OrderDate TIMESTAMP,
        PurchaseAddress STRING,
        City STRING,
        State STRING,
        ReportYear INT,
        months INT
        )
    USING PARQUET
    PARTITIONED BY(ReportYear, months)
    options('compression'='snappy') 
    location 'dbfs:/FileStore/salesdata4/published'
""")

# COMMAND ----------

spark.sql(""" INSERT INTO sales3
        select OrderId,
                Product,
                QuantityOrdered,
                PriceEach,
                OrderDate,
                PurchaseAddress,
                City,
                State,
                ReportYear,
                month
        from temp_sales""")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sales3
# MAGIC WHERE OrderId IS NOT NULL 
# MAGIC   AND Product IS NOT NULL 
# MAGIC   AND QuantityOrdered IS NOT NULL 
# MAGIC   AND PriceEach IS NOT NULL 
# MAGIC   AND OrderDate IS NOT NULL 
# MAGIC   AND PurchaseAddress IS NOT NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC # From the sales3 find out the no of orderplaced in newyork

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC SELECT COUNT(*) AS OrderCount
# MAGIC FROM sales3
# MAGIC WHERE City = 'New York City';

# COMMAND ----------

# MAGIC %md
# MAGIC # From the sales3 using the group by find out the no of orderplaced in each city

# COMMAND ----------

# MAGIC %sql
# MAGIC  
# MAGIC SELECT City, COUNT(*) AS OrderCount FROM sales3 GROUP BY City ORDER BY OrderCount DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Which is the best month for sales

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT months, round(SUM(priceeach * QuantityOrdered),2) AS TotalSales
# MAGIC FROM sales3
# MAGIC GROUP BY months
# MAGIC ORDER BY TotalSales DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC # Which city sold the most product

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT City,sum(QuantityOrdered) as TotalProducts from sales3 group by City order by TotalProducts desc;

# COMMAND ----------

# MAGIC %md
# MAGIC #1 Determine the most popular product based on quantity sold:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Product, SUM(QuantityOrdered) AS TotalQuantity
# MAGIC FROM sales3
# MAGIC GROUP BY Product
# MAGIC ORDER BY TotalQuantity DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #2 Calculate the average value of an order to understand spending behavior:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ROUND(AVG(priceeach * QuantityOrdered), 2) AS AvgOrderValue
# MAGIC FROM sales3;

# COMMAND ----------

# MAGIC %md
# MAGIC #3 Analyze how sales change throughout the year:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT months, COUNT(*) AS OrderCount
# MAGIC FROM sales3
# MAGIC GROUP BY months
# MAGIC ORDER BY months ASC;

# COMMAND ----------

# MAGIC %md
# MAGIC #4 Check whether certain cities prefer particular products:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT City, Product, SUM(QuantityOrdered) AS TotalQuantity
# MAGIC FROM sales3
# MAGIC GROUP BY City, Product
# MAGIC ORDER BY City, TotalQuantity DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #5 Identify frequently bought-together items (requires transaction-based data):

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT OrderId, Product
# MAGIC FROM sales3
# MAGIC ORDER BY OrderId;

# COMMAND ----------

# MAGIC %md
# MAGIC #6 Find customers who have made multiple purchases:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CustomerID, COUNT(OrderId) AS OrderCount
# MAGIC FROM sales3
# MAGIC GROUP BY CustomerID
# MAGIC ORDER BY OrderCount DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC #7 Find out which products contribute the most revenue:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Product, SUM(priceeach * QuantityOrdered) AS TotalRevenue
# MAGIC FROM sales3
# MAGIC GROUP BY Product
# MAGIC ORDER BY TotalRevenue DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC #8 Identify which products are frequently returned (if you have a ReturnFlag column):

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Product, COUNT(*) AS ReturnCount
# MAGIC FROM sales3
# MAGIC WHERE ReturnFlag = 'Yes'
# MAGIC GROUP BY Product
# MAGIC ORDER BY ReturnCount DESC;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #9 Identify the customers who spend the most:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CustomerID, SUM(priceeach * QuantityOrdered) AS TotalSpent
# MAGIC FROM sales3
# MAGIC GROUP BY CustomerID
# MAGIC ORDER BY TotalSpent DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC #10 Calculate how much sales are growing month-over-month:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT months, 
# MAGIC        SUM(priceeach * QuantityOrdered) AS TotalSales, 
# MAGIC        LAG(SUM(priceeach * QuantityOrdered), 1) OVER (ORDER BY months) AS PreviousMonthSales,
# MAGIC        ROUND((SUM(priceeach * QuantityOrdered) - LAG(SUM(priceeach * QuantityOrdered), 1) OVER (ORDER BY months)) / LAG(SUM(priceeach * QuantityOrdered), 1) OVER (ORDER BY months) * 100, 2) AS GrowthRate
# MAGIC FROM sales3
# MAGIC GROUP BY months
# MAGIC ORDER BY months;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe sales3;

# COMMAND ----------

# MAGIC %md
# MAGIC Count the orderid using groupby

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT OrderId, COUNT(*) AS OrderCount
# MAGIC FROM sales3
# MAGIC GROUP BY OrderId;