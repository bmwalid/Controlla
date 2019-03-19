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
spark = SparkSession.builder.appName("010-mds_detail_elt_gestion").config(
    conf=SparkConf()).enableHiveSupport().getOrCreate()

# Data path
path = "s3://decathlon-ods/ods/mds_detail_elt_gestion/*.gz"

# From gz files on S3 to Spark Dataframe
df = spark.read.option("header", "false").option("delimiter", "|").csv(path)

# write to hdfs
df.select("_c0", "_c3", "_c9") \
    .withColumn("elg_num_elt_gestion_elg", df["_c0"].cast(IntegerType())) \
    .withColumn("ege_basique", df["_c3"].cast(StringType())) \
    .withColumn("ege_security_product", df["_c9"].cast(StringType())) \
    .select("elg_num_elt_gestion_elg", "ege_basique", "ege_security_product") \
    .repartition(80).write.option("compression", "snappy").mode("overwrite").format("parquet").saveAsTable(
    "kylin_usb_mqb.mds_detail_elt_gestion")

# stopping session
spark.sparkContext.stop()
