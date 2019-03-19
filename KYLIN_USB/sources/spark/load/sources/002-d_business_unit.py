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
spark = SparkSession.builder.appName("002-d_business_unit").config(conf=SparkConf()).enableHiveSupport().getOrCreate()

# Data path
path = "s3://decathlon-cds/cds/d_business_unit/*.gz"

# From gz files on S3 to Spark Dataframe
df = spark.read.option("header", "false").option("delimiter", "|").csv(path)

# write to hdfs
df.select("_c0", "_c22", "_c2") \
    .withColumn("but_idr_business_unit", df["_c0"].cast(IntegerType())) \
    .withColumn("but_name_business_unit", df["_c22"].cast(StringType())) \
    .withColumn("but_num_business_unit", df["_c2"].cast(IntegerType())) \
    .select("but_idr_business_unit", "but_name_business_unit", "but_num_business_unit") \
    .repartition(80).write.option("compression", "snappy").mode("overwrite").format("parquet").saveAsTable(
    "kylin_usb_mqb.d_business_unit")

# stopping session
spark.sparkContext.stop()
