# import Libraries
from pyspark.context import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *

# init sparkConf
conf = SparkConf()
conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .set("spark.executor.cores", "1") \
    .set("spark.executor.memory", "1G")

# Initialize Spark Session
spark = SparkSession.builder.appName("024-nwt_itv_def_loc").config(conf=SparkConf()).enableHiveSupport().getOrCreate()

# Data path
path = "s3://decathlon-ods/ods_retail/nwt_itv_def_loc/*.gz"

# From gz files on S3 to Spark Dataframe
df = spark.read.option("header", "false").option("delimiter", "|").csv(path)

# write to hdfs
df.select("_c4", "_c3", "_c2", "_c1", "_c0") \
    .withColumn("idl_category_id", df["_c4"].cast(IntegerType())) \
    .withColumn("idl_default_id", df["_c3"].cast(IntegerType())) \
    .withColumn("idl_localisation_id", df["_c2"].cast(IntegerType())) \
    .withColumn("itv_id", df["_c1"].cast(IntegerType())) \
    .withColumn("osr_server_alias_name", df["_c0"].cast(StringType())) \
    .select("idl_category_id", "idl_default_id", "idl_localisation_id", "itv_id", "osr_server_alias_name") \
    .repartition(80).write.option("compression", "snappy").mode("overwrite").format("parquet").saveAsTable(
    "kylin_usb_mqb.nwt_itv_def_loc")

# stopping session
spark.sparkContext.stop()
