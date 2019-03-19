# import Libraries
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *

# init sparkConf
conf = SparkConf()
conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .set("spark.executor.cores", "1") \
    .set("spark.executor.memory", "1G")

# Initialize Spark Session
spark = SparkSession.builder.appName("009-wrf1").config(conf=SparkConf()).enableHiveSupport().getOrCreate()

# Data path
path = "s3://decathlon-cds/cds_supply/site_attributs_0plant_cds/site_attributs_0plant_wrf1_cds/*.gz"

# From gz files on S3 to Spark Dataframe
df = spark.read.option("header", "false").option("delimiter", "|").csv(path)

# write to hdfs
df.select("_c1", "_c22") \
    .withColumn("cust_num_plant", df["_c1"].cast(StringType())) \
    .withColumn("distrib_channel", df["_c22"].cast(StringType())) \
    .select("cust_num_plant", "distrib_channel") \
    .repartition(80).write.option("compression", "snappy").mode("overwrite").format("parquet").saveAsTable(
    "kylin_usb_mqb.wrf1")

# stopping session
spark.sparkContext.stop()
