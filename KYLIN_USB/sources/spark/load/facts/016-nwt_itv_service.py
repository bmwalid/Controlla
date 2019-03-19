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
spark = SparkSession.builder.appName("016-nwt_itv_service").config(conf=SparkConf()).enableHiveSupport().getOrCreate()

# Data path
path = "s3://decathlon-ods/ods_retail/nwt_itv_service/*.gz"

# From gz files on S3 to Spark Dataframe
df = spark.read.option("header", "false").option("delimiter", "|").csv(path)

# write to hdfs
df.select("_c3", "_c10", "_c11", "_c7", "_c0", "_c12") \
    .withColumn("isv_quantity", df["_c3"].cast(IntegerType())) \
    .withColumn("isv_id_operation_status", df["_c10"].cast(IntegerType())) \
    .withColumn("isv_unit_bought_price", df["_c11"].cast(FloatType())) \
    .withColumn("isv_id_intervention", df["_c7"].cast(IntegerType())) \
    .withColumn("osr_server_alias_name", df["_c0"].cast(StringType())) \
    .withColumn("isv_code_article", df["_c12"].cast(IntegerType())) \
    .select("isv_quantity", "isv_id_operation_status", "isv_unit_bought_price", "isv_id_intervention",
            "osr_server_alias_name", "isv_code_article") \
    .repartition(80).write.option("compression", "snappy").mode("overwrite").format("parquet").saveAsTable(
    "kylin_usb_mqb.nwt_itv_service")

# stopping session
spark.sparkContext.stop()
