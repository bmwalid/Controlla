# import Libraries
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# init sparkConf
conf = SparkConf()
conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .set("spark.executor.cores", "1") \
    .set("spark.executor.memory", "1G")

# Initialize Spark Session
spark = SparkSession.builder.appName("034-dk_product").config(
    conf=SparkConf()).enableHiveSupport().getOrCreate()

# Data path
path1 = "s3://decathlon-cds/cds_supply/decathlon_product_cds/characteristic_values_cds/*.gz"
path2 = "s3://decathlon-cds/cds_supply/decathlon_product_cds/link_internal_id_object_cds/*.gz"
# From gz files on S3 to Spark Dataframe
cvc = spark.read.option("header", "false").option("delimiter", "|").csv(path1)
lio = spark.read.option("header", "false").option("delimiter", "|").csv(path2)


cvc = cvc.select("_c0", "_c1", "_c6")\
         .withColumn("OBJEK", cvc["_c0"].cast(IntegerType()))\
         .withColumn("ATINN", cvc["_c1"].cast(IntegerType()))\
         .withColumn("ATWRT", cvc["_c6"].cast(StringType()))\
         .select("OBJEK", "ATINN", "ATWRT")

cvc = cvc.filter(col("ATINN")==7).select("OBJEK", "ATWRT").drop_duplicates()

lio = lio.select("_c0", "_c2", "_c3", "_c4", "_c5")\
         .withColumn("CUOBJ", lio["_c0"].cast(IntegerType()))\
         .withColumn("OBTAB", lio["_c2"].cast(StringType()))\
         .withColumn("OBJEK", lio["_c3"].cast(IntegerType()))\
         .withColumn("ROBTAB", lio["_c4"].cast(StringType()))\
         .withColumn("ROBJEK", lio["_c5"].cast(IntegerType()))\
         .select("CUOBJ", "OBTAB", "OBJEK", "ROBTAB", "ROBJEK")

lio = lio.filter(col("OBTAB")=='MARAT').filter(col("ROBTAB")=='MARA')\
         .select("CUOBJ", "OBJEK", "ROBJEK").drop_duplicates()


df = lio.join(cvc, how='left', on=[lio.CUOBJ == cvc.OBJEK])\
        .select(lio.OBJEK, lio.ROBJEK, cvc.ATWRT)\
        .drop_duplicates()

df.repartition(80).write.option("compression", "snappy").mode("overwrite").format("parquet")\
  .saveAsTable("kylin_usb_mqb.dk_product")

# stopping session
spark.sparkContext.stop()

