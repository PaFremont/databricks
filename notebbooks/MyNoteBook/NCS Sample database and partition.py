# Databricks notebook source
inputPath = "/mnt/hnsgen2/samplecsv/data.csv"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType
from pyspark.sql.functions import col

inputSchema = StructType([
  StructField("InvoiceNo", IntegerType(), True),
  StructField("StockCode", StringType(), True),
  StructField("Description", StringType(), True),
  StructField("Quantity", IntegerType(), True),
  StructField("InvoiceDate", StringType(), True),
  StructField("UnitPrice", DoubleType(), True),
  StructField("CustomerID", IntegerType(), True),
  StructField("Country", StringType(), True)
])

rawDataDF = (spark.read 
  .option("header", "true")
  .schema(inputSchema)
  .csv(inputPath) 
)

# COMMAND ----------

jdbcHostname = "pfasql.database.windows.net"
jdbcPort = 1433
jdbcDatabase = "pfasql"
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)
connectionProps = {
  "user": "pafrem",
  "password": dbutils.secrets.get(scope = "ETL", key = "asql")
}

# COMMAND ----------

# MAGIC %md
# MAGIC Populate a SQL table

# COMMAND ----------

rawDataDF.write.jdbc(url=jdbcUrl, table="dbo.Databricks_Customer_Data", mode="overwrite", properties=connectionProps)

# COMMAND ----------

CustomerDataDF = spark.read.jdbc(url=jdbcUrl, table="dbo.Databricks_Customer_Data", properties=connectionProps)
display(CustomerDataDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Manage partitions

# COMMAND ----------

from pyspark.sql.functions import min, max

dfMin = CustomerDataDF.select(min("InvoiceNo")).first()[0]
dfMax = CustomerDataDF.select(max("invoiceNo")).first()[0]

print("DataFrame minimum: {}\nDataFrame maximum: {}".format(dfMin, dfMax))

# COMMAND ----------

CustomerDataDFParallel = spark.read.jdbc(
  url=jdbcUrl, 
  table="dbo.Databricks_Customer_Data",
  column='"InvoiceNo"',
  lowerBound=dfMin,
  upperBound=dfMax,
  numPartitions=12,
  properties=connectionProps
)

display(CustomerDataDFParallel)

# COMMAND ----------

print(CustomerDataDF.rdd.getNumPartitions())
print(CustomerDataDFParallel.rdd.getNumPartitions())

# COMMAND ----------

pushdown_query = "(update dbo.Databricks_Customer_Data set Quantity=2 where InvoiceNo = 541516) cdatalias"
df = spark.read.jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProps)
display(df)


# COMMAND ----------

spark-submit --version

# COMMAND ----------

# MAGIC %sh 
# MAGIC pip install --upgrade pip

# COMMAND ----------

