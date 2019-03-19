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
spark = SparkSession.builder.appName("017-nwt_itv_spare_part").config(
    conf=SparkConf()).enableHiveSupport().getOrCreate()

# Data path
path = "s3://decathlon-ods/ods_retail/nwt_itv_spare_part/*.gz"

# From gz files on S3 to Spark Dataframe
df = spark.read.option("header", "false").option("delimiter", "|").csv(path)

# write to hdfs
df.select("_c2", "_c10", "_c11", "_c5", "_c0", "_c3") \
    .withColumn("isp_quantity", df["_c2"].cast(IntegerType())) \
    .withColumn("isp_id_operation_status", df["_c10"].cast(IntegerType())) \
    .withColumn("isv_unit_bought_price", df["_c11"].cast(FloatType())) \
    .withColumn("isp_id_intervention", df["_c5"].cast(IntegerType())) \
    .withColumn("osr_server_alias_name", df["_c0"].cast(StringType())) \
    .withColumn("isp_internal_code", df["_c3"].cast(IntegerType())) \
    .select("isp_quantity", "isp_id_operation_status", "isv_unit_bought_price", "isp_id_intervention",
            "osr_server_alias_name", "isp_internal_code") \
    .repartition(80).write.option("compression", "snappy").mode("overwrite").format("parquet").saveAsTable(
    "kylin_usb_mqb.nwt_itv_spare_parts")

# stopping session
spark.sparkContext.stop()
