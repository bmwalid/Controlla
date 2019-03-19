# import Libraries
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.window import Window
# init sparkConf
conf = SparkConf()
conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .set("spark.executor.cores", "1") \
    .set("spark.executor.memory", "1G")

# Initialize Spark Session
spark = SparkSession.builder.appName("029-quotas_fournisseurs").config(
    conf=SparkConf()).enableHiveSupport().getOrCreate()

# Data path
path = "s3://decathlon-cds/cds_supply/quotas_fournisseurs/quotas_fournisseurs/*.gz"

# From gz files on S3 to Spark Dataframe
df = spark.read.option("header", "false").option("delimiter", "|").csv(path)

# write to hdfs
df.select("_c0", "_c1", "_c2", "_c3", "_c4", "_c5", "_c6", "_c7", "_c8") \
    .withColumn("material_id", df["_c0"].cast(IntegerType())) \
    .withColumn("purch_org", df["_c1"].cast(StringType())) \
    .withColumn("vendor_id", df["_c2"].cast(IntegerType())) \
    .withColumn("date_max", df["_c3"].cast(DateType())) \
    .withColumn("currency_id", df["_c4"].cast(StringType())) \
    .withColumn("max_cmde", df["_c5"].cast(StringType())) \
    .withColumn("price_cession", df["_c6"].cast(FloatType())) \
    .withColumn("mdl_num_model_r3", df["_c7"].cast(IntegerType())) \
    .withColumn("subcontractor_id", df["_c8"].cast(IntegerType())) \
    .select("material_id", "purch_org", "vendor_id", "date_max", "currency_id", "max_cmde", "price_cession",
            "mdl_num_model_r3", "subcontractor_id")\
    .withColumn("rn", F.row_number().over(Window.partitionBy("material_id", "mdl_num_model_r3", "purch_org").orderBy(F.desc("date_max"))))\
    .filter((F.col("rn") == 1) & (~F.col("purch_org").isNull()))\
    .repartition(80).write.option("compression", "snappy")\
    .mode("overwrite").format("parquet").saveAsTable("kylin_usb_mqb.quotas_fournisseurs")

# stopping session
spark.sparkContext.stop()
