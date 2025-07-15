# Databricks notebook source
# DBTITLE 1,Mounting data
# MOUNTING DATA
dbutils.fs.mount(
    source = "wasbs://retail-container@retailprojadls1.blob.core.windows.net",
    mount_point= "/mnt/retail-container",
    extra_configs= {"fs.azure.account.key.retailprojadls1.blob.core.windows.net":{ENTER KEY HERE}}
)

# COMMAND ----------

# DBTITLE 1,Listing Files post mounting
# LISTING FILES
dbutils.fs.ls('/mnt/retail-container/Bronze/')
dbutils.fs.ls('/mnt/retail-container/Bronze/Transaction')
dbutils.fs.ls('/mnt/retail-container/Bronze/Customer')

# COMMAND ----------

# DBTITLE 1,Reading Bronze Layer
# READING PARQUETS AS DATAFRAMES - SOURCE SYSTEM: Azure SQL DB
df_transactions = spark.read.parquet("/mnt/retail-container/Bronze/Transaction")
df_stores = spark.read.parquet("/mnt/retail-container/Bronze/Store")
df_products = spark.read.parquet("/mnt/retail-container/Bronze/Product")

# READING PARQUETS AS DATAFRAMES - SOURCE SYSTEM: JSON
df_customers = spark.read.parquet("/mnt/retail-container/Bronze/Customer/rohitpaul1998/Retail-Azure-Data-Engineering/refs/heads/main/data-source/")

display(df_transactions)

# COMMAND ----------

# DESCRIBING COLUMN CHARACTERISTICS
# display(df_transactions.dtypes)
# display(df_customers.dtypes)
# display(df_stores.dtypes)
# display(df_products.dtypes)

# COMMAND ----------

# DBTITLE 1,Creating Silver Layer - data cleaning
from pyspark.sql.functions import col

# CONVERTING TYPES & CLEANING DATA
df_transactions = df_transactions.select(
    col("transaction_id").cast("int"),
    col("customer_id").cast("int"),
    col("product_id").cast("int"),
    col("store_id").cast("int"),
    col("quantity").cast("int"),
    col("transaction_date").cast("date")
)

df_products = df_products.select(
    col("product_id").cast("int"),
    col("product_name"),
    col("category"),
    col("price").cast("double")
)

df_stores = df_stores.select(
    col("store_id").cast("int"),
    col("store_name"),
    col("location")
)

# removing duplicate values in customer_id
df_customers = df_customers.select("customer_id","first_name","last_name","email","city","registration_date").dropDuplicates(["customer_id"])

# COMMAND ----------

# DBTITLE 1,Joining All Data Together
# JUST LIKE HOW WE WOULD JOIN TWO OR MORE SQL TABLES TOGETHER, WE ARE JOINING DATAFRAMES IN SPARK USING THE JOIN COMMAND

df_silver = df_transactions \
    .join(df_customers, "customer_id") \
    .join(df_products, "product_id") \
    .join(df_stores, "store_id") \
    .withColumn("total_amount", col("quantity") * col("price"))

display(df_silver)

# COMMAND ----------

# DBTITLE 1,Dumping the joined data to ADLS Silver Directory
silver_path = "/mnt/retail-container/Silver/" # Path to store the cleaned (silver layer) data

df_silver.write.mode("overwrite").format("delta").save(silver_path) # Saved as Delta (best practice for cleaned data) using overwrite mode to avoid duplicates and always store fresh data.

# COMMAND ----------

# DBTITLE 1,Querying the Delta file stored in Silver Directory
# Using a temporary view to query the silver layer data
df_silver.createOrReplaceTempView("retail_silver_temp_vw")
spark.sql("SELECT * FROM retail_silver_temp_vw").display()
spark.sql("SELECT COUNT(*) FROM retail_silver_temp_vw").display()

# COMMAND ----------

# LOADING CLEANSED DATA FROM SILVER LAYER
silver_df = spark.read.format("delta").load(silver_path)
display(silver_df)

# COMMAND ----------

# DBTITLE 1,Gold Layer
# Business REQ1: Total number of products got sold?
# Business REQ2: What is the total sales amount?
# Business REQ3: What are the number of transactions?
# Business REQ4: What is the average value of each transaction?

from pyspark.sql.functions import sum, countDistinct, avg
gold_df = silver_df.groupBy(
    "transaction_date", "product_id", "product_name", "category", "store_id", "store_name", "location"
).agg(
    sum("quantity").alias("Total_Quantities_Sold"),
    sum("total_amount").alias("Total_Sales_Amount"),
    countDistinct("transaction_id").alias("Number_Of_Transactions"),
    avg("total_amount").alias("Average_Transaction_Value")
)

# COMMAND ----------

# Displaying the gold dataframe to check refined data from the silver layer
display(gold_df)

# COMMAND ----------

# DBTITLE 1,Dumping Refined Gold Data into Gold Path
gold_path = "/mnt/retail-container/Gold/" # Path to store the refined (gold layer) data
gold_df.write.mode("overwrite").format("delta").save(gold_path) 

# COMMAND ----------

# DBTITLE 1,Querying Stored Refined Data from Gold Directory
# Creating a temp view
gold_df.createOrReplaceTempView("retail_gold_temp_vw")
spark.sql("SELECT * FROM retail_gold_temp_vw").display()
spark.sql("SELECT COUNT(*) FROM retail_gold_temp_vw").display()