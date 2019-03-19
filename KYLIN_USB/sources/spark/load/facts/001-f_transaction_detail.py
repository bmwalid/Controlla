# import Libraries
import sys
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
from pyspark.sql.types import *
from pyspark.sql.functions import *


years = sys.argv[1]
months = sys.argv[2]

# init sparkConf
conf = SparkConf()
conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# Initialize Spark Session
spark = SparkSession.builder.appName("001-f_transaction_detail_" + years + months) \
    .config(conf=SparkConf()) \
    .enableHiveSupport() \
    .getOrCreate()

sc = spark.sparkContext
sqlContext = HiveContext(sc)
sqlContext.setConf("hive.exec.dynamic.partition", "true")
sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
# Data path
path = "s3://decathlon-cds/cds/f_transaction_detail/" + years + months + "/*.gz"

years = str(int(years))
months = str(int(months))
# From gz files on S3 to Spark Dataframe
df = spark.read.option("header", "false").option("delimiter", "|").csv(path)

# write to hdfs
df.select("_c0", "_c1", "_c6", "_c12", "_c9", "_c53", "_c35") \
    .withColumn("the_transaction_id", df["_c0"].cast(StringType())) \
    .withColumn("tdt_num_line", df["_c1"].cast(IntegerType())) \
    .withColumn("day_transaction", dayofmonth(df["_c6"].cast(TimestampType())).cast(StringType())) \
    .withColumn("but_idr_business_unit", df["_c12"].cast(IntegerType())) \
    .withColumn("sku_idr_sku", df["_c9"].cast(IntegerType())) \
    .withColumn("f_qty_item", df["_c53"].cast(IntegerType())) \
    .withColumn("tdt_type_detail", df["_c35"].cast(StringType())) \
    .withColumn("year_transaction", year(df["_c6"].cast(TimestampType())).cast(StringType())) \
    .withColumn("month_transaction", month(df["_c6"].cast(TimestampType())).cast(StringType())) \
    .select("the_transaction_id", "tdt_num_line","but_idr_business_unit", "sku_idr_sku",
            "f_qty_item", "tdt_type_detail", "year_transaction", "month_transaction","day_transaction") \
    .repartition(10).write.option("compression", "snappy").mode("overwrite") \
    .insertInto("kylin_usb_mqb.f_transaction_detail")

# stopping session
spark.sparkContext.stop()
