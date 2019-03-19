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
spark = SparkSession.builder.appName("021-mds_libelle_traduction").config(
    conf=SparkConf()).enableHiveSupport().getOrCreate()

# Data path
path = "s3://decathlon-ods/ods/mds_libelle_traduction/*.gz"

# From gz files on S3 to Spark Dataframe
df = spark.read.option("header", "false").option("delimiter", "|").csv(path)

# write to hdfs
df.select("_c1", "_c4", "_c0", "_c2") \
    .withColumn("lib_num_libelle_lib", df["_c1"].cast(IntegerType())) \
    .withColumn("lit_libelle_long", df["_c4"].cast(StringType())) \
    .withColumn("tlb_typ_libelle_lib", df["_c0"].cast(StringType())) \
    .withColumn("lan_code_langue_lan", df["_c2"].cast(StringType())) \
    .select("lib_num_libelle_lib", "lit_libelle_long", "tlb_typ_libelle_lib", "lan_code_langue_lan") \
    .repartition(80).write.option("compression", "snappy").mode("overwrite").format("parquet").saveAsTable(
    "kylin_usb_mqb.mds_libelle_traduction")

# stopping session
spark.sparkContext.stop()
