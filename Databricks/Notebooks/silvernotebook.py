# Databricks notebook source
# MAGIC %md
# MAGIC #Data Access from ADLS gen 2

# COMMAND ----------

secret = "M9H8Q~62GzMbpcOuNxVGITAeZCxUH**12~mDqP63~bB~"
app_id = "b6996712-9cc3-4769-87fd-e136fcda79da"
tenant_id = "ed6f6a72-88ea-486c-90d9-12cf3bea00d2"

# COMMAND ----------


spark.conf.set("fs.azure.account.auth.type.jpprojectdatastorageadls.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.jpprojectdatastorageadls.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.jpprojectdatastorageadls.dfs.core.windows.net", "b6996712-9cc3-4769-87fd-sdfe**e136fcda79da")
spark.conf.set("fs.azure.account.oauth2.client.secret.jpprojectdatastorageadls.dfs.core.windows.net", 'M9H8Q~62GzMbpcOuNxVGI**^&TAeZCxUH~mDqP63~bB~')
spark.conf.set("fs.azure.account.oauth2.client.endpoint.jpprojectdatastorageadls.dfs.core.windows.net", "https://login.microsoftonline.com/ed6f6a72-88ea-486c-90d9-12cf3bea00d2/oauth2/token")

# COMMAND ----------

dbutils.fs.ls('abfss://nyctaxidata@jpprojectdatastorageadls.dfs.core.windows.net/')

# COMMAND ----------

# MAGIC %md
# MAGIC #Data reading

# COMMAND ----------

# MAGIC %md
# MAGIC **Importing Libraries**

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading csv data**

# COMMAND ----------

# MAGIC %md
# MAGIC **Trip zone data**

# COMMAND ----------

df_trip_zone = spark.read.format('csv')\
    .option('inferSchema', 'True')\
        .option('header',True)\
            .load('abfss://nyctaxidata@jpprojectdatastorageadls.dfs.core.windows.net/alldata/trip-zone')

# COMMAND ----------

df_trip_zone.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Trip data- using recursivefileLookup()**

# COMMAND ----------

df_trip = spark.read.format('parquet')\
    .option('mergeSChema', 'True')\
        .option('header',True)\
            .option('recursiveFileLookup','true')\
            .load('abfss://nyctaxidata@jpprojectdatastorageadls.dfs.core.windows.net/alldata/trip-data')

# COMMAND ----------

df_good = spark.read.parquet("abfss://nyctaxidata@jpprojectdatastorageadls.dfs.core.windows.net/alldata/trip-data/green_tripdata_2023-01.parquet")
df_good.printSchema()

# COMMAND ----------

from pyspark.sql.types import *

trip_schema = StructType([
    StructField("VendorID", LongType()),
    StructField("lpep_pickup_datetime", TimestampType()),
    StructField("lpep_dropoff_datetime", TimestampType()),
    StructField("store_and_fwd_flag", StringType()),
    StructField("RatecodeID", DoubleType()),
    StructField("PULocationID", LongType()),
    StructField("DOLocationID", LongType()),
    StructField("passenger_count", DoubleType()),
    StructField("trip_distance", DoubleType()),
    StructField("fare_amount", DoubleType()),
    StructField("extra", DoubleType()),
    StructField("mta_tax", DoubleType()),
    StructField("tip_amount", DoubleType()),
    StructField("tolls_amount", DoubleType()),
    StructField("ehail_fee", IntegerType()),
    StructField("improvement_surcharge", DoubleType()),
    StructField("total_amount", DoubleType()),
    StructField("payment_type", DoubleType()),
    StructField("trip_type", DoubleType()),
    StructField("congestion_surcharge", DoubleType())
])

# COMMAND ----------

df_trip = (
    spark.read
        .schema(trip_schema)
        .option("recursiveFileLookup", "true")
        .parquet("abfss://nyctaxidata@jpprojectdatastorageadls.dfs.core.windows.net/alldata/trip-data")
)

# COMMAND ----------

files = dbutils.fs.ls("abfss://nyctaxidata@jpprojectdatastorageadls.dfs.core.windows.net/alldata/trip-data")

# COMMAND ----------

bad_files = []
good_files = []

for f in files:
    try:
        spark.read.parquet(f.path).limit(1).collect()
        good_files.append(f.path)
    except Exception as e:
        bad_files.append(f.path)

# COMMAND ----------

bad_files

# COMMAND ----------

good_files

# COMMAND ----------

# MAGIC %md
# MAGIC #Making a new dir#

# COMMAND ----------

dbutils.fs.mkdirs("abfss://nyctaxidata@jpprojectdatastorageadls.dfs.core.windows.net/clean-trip-data")

# COMMAND ----------

dbutils.fs.ls("abfss://nyctaxidata@jpprojectdatastorageadls.dfs.core.windows.net/")

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql import functions as F

trip_schema = StructType([
    StructField("VendorID", LongType()),
    StructField("lpep_pickup_datetime", TimestampType()),
    StructField("lpep_dropoff_datetime", TimestampType()),
    StructField("store_and_fwd_flag", StringType()),
    StructField("RatecodeID", DoubleType()),
    StructField("PULocationID", LongType()),
    StructField("DOLocationID", LongType()),
    StructField("passenger_count", DoubleType()),
    StructField("trip_distance", DoubleType()),
    StructField("fare_amount", DoubleType()),
    StructField("extra", DoubleType()),
    StructField("mta_tax", DoubleType()),
    StructField("tip_amount", DoubleType()),
    StructField("tolls_amount", DoubleType()),
    StructField("ehail_fee", IntegerType()),
    StructField("improvement_surcharge", DoubleType()),
    StructField("total_amount", DoubleType()),
    StructField("payment_type", DoubleType()),
    StructField("trip_type", DoubleType()),
    StructField("congestion_surcharge", DoubleType())
])


clean_path = "abfss://nyctaxidata@jpprojectdatastorageadls.dfs.core.windows.net/clean-trip-data"

for f in good_files:
    print("Processing:", f)
    df = spark.read.parquet(f)

    # Cast all columns

# COMMAND ----------

dbutils.fs.ls(clean_path)

# COMMAND ----------

from pyspark.sql import functions as F

clean_path = "abfss://nyctaxidata@jpprojectdatastorageadls.dfs.core.windows.net/clean-trip-data"

for f in good_files:
    print(f"Cleaning: {f}")
    
    df = spark.read.parquet(f)

    df_clean = (
        df
        .withColumn("VendorID", F.col("VendorID").cast("long"))
        .withColumn("lpep_pickup_datetime", F.col("lpep_pickup_datetime").cast("timestamp"))
        .withColumn("lpep_dropoff_datetime", F.col("lpep_dropoff_datetime").cast("timestamp"))
        .withColumn("store_and_fwd_flag", F.col("store_and_fwd_flag").cast("string"))
        .withColumn("RatecodeID", F.col("RatecodeID").cast("double"))
        .withColumn("PULocationID", F.col("PULocationID").cast("long"))
        .withColumn("DOLocationID", F.col("DOLocationID").cast("long"))
        .withColumn("passenger_count", F.col("passenger_count").cast("double"))
        .withColumn("trip_distance", F.col("trip_distance").cast("double"))
        .withColumn("fare_amount", F.col("fare_amount").cast("double"))
        .withColumn("extra", F.col("extra").cast("double"))
        .withColumn("mta_tax", F.col("mta_tax").cast("double"))
        .withColumn("tip_amount", F.col("tip_amount").cast("double"))
        .withColumn("tolls_amount", F.col("tolls_amount").cast("double"))
        .withColumn("ehail_fee", F.col("ehail_fee").cast("int"))
        .withColumn("improvement_surcharge", F.col("improvement_surcharge").cast("double"))
        .withColumn("total_amount", F.col("total_amount").cast("double"))
        .withColumn("payment_type", F.col("payment_type").cast("double"))
        .withColumn("trip_type", F.col("trip_type").cast("double"))
        .withColumn("congestion_surcharge", F.col("congestion_surcharge").cast("double"))
    )

    df_clean.write.mode("append").parquet(clean_path)

# COMMAND ----------

df_trip = spark.read.parquet(clean_path)
df_trip.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Data Transformation#

# COMMAND ----------

# MAGIC %md
# MAGIC **Pushing the Trip zone data**

# COMMAND ----------

df_trip_zone.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Transformaing the Zone column absurd values to a single value**

# COMMAND ----------

# MAGIC %md
# MAGIC **Splitting the Zone column**

# COMMAND ----------

from pyspark.sql.functions import split, col
df_trip_zone = df_trip_zone.withColumn('zone1',split(col('Zone'),'/')[0])\
    .withColumn('zone2',split(col('Zone'),'/')[1])
df_trip_zone.display()

# COMMAND ----------

df_trip_zone.write.format('parquet')\
    .mode('append')\
        .option('path','abfss://silverlayer@jpprojectdatastorageadls.dfs.core.windows.net/trip_zone')\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC **Pushing Trip Data**

# COMMAND ----------

df_trip.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Splitting full date time column to date year month**

# COMMAND ----------

from pyspark.sql.functions import to_date, year, month, col
df_trip = df_trip.withColumn('trip_date',to_date(col("lpep_pickup_datetime")))\
        .withColumn('trip_year',year(col("lpep_pickup_datetime")))\
            .withColumn('trip_month',month(col("lpep_pickup_datetime")))

# COMMAND ----------

df_trip.display()

# COMMAND ----------

df_trip = df_trip.select('VendorID','PULocationID','DOLocationID','trip_year','trip_month','trip_date','RatecodeID','store_and_fwd_flag','passenger_count','trip_distance','fare_amount','extra','mta_tax','tip_amount','tolls_amount','total_amount')



# COMMAND ----------

df_trip.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **write to silver layer**

# COMMAND ----------

df_trip.write.format('parquet')\
    .mode('append')\
        .option('path','abfss://silverlayer@jpprojectdatastorageadls.dfs.core.windows.net/trip_data')\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Analysis##

# COMMAND ----------

display(df_trip)

# COMMAND ----------

