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
spark = SparkSession.builder.appName("006-t001w").config(conf=SparkConf()).enableHiveSupport().getOrCreate()

# Data path
path = "s3://decathlon-cds/cds_supply/site_attributs_0plant_cds/site_attributs_0plant_t001w_cds/*.gz"

# From gz files on S3 to Spark Dataframe
df = spark.read.option("header", "false").option("delimiter", "|").csv(path)

# write to hdfs
df.select("_c1", "_c7", "_c8", "_c15", "_c4", "_c2", "_c20", "_c13") \
    .withColumn("plant_id", df["_c1"].cast(StringType())) \
    .withColumn("purch_org", df["_c7"].cast(StringType())) \
    .withColumn("sales_org", df["_c8"].cast(StringType())) \
    .withColumn("distrib_channel", df["_c15"].cast(StringType())) \
    .withColumn("cust_num_plant", df["_c4"].cast(StringType())) \
    .withColumn("sapsrc", df["_c2"].cast(StringType())) \
    .withColumn("plant_category", df["_c20"].cast(StringType())) \
    .withColumn("address_num", df["_c13"].cast(IntegerType())) \
    .select("plant_id", "purch_org", "sales_org", "distrib_channel", "cust_num_plant", "sapsrc", "plant_category",
            "address_num") \
    .repartition(80).write.option("compression", "snappy").mode("overwrite").format("parquet").saveAsTable(
    "kylin_usb_mqb.t001w")

# stopping session
spark.sparkContext.stop()
