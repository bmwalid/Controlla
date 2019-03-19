# import Libraries
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import HiveContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import *

# init sparkConf
conf = SparkConf()
conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \

# Initialize Spark Session
spark = SparkSession.builder.appName("003-d_sku_h").config(conf=SparkConf()).enableHiveSupport().getOrCreate()
sc = spark.sparkContext
sqlContext = HiveContext(sc)
sqlContext.setConf("hive.exec.dynamic.partition", "true")
sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

# Data path
path = "s3://decathlon-cds/cds/d_sku_h/*.gz"

# From gz files on S3 to Spark Dataframe
df = spark.read.option("header", "false").option("delimiter", "|").csv(path)

# Transform
df = df.select("_c0", "_c1", "_c45", "_c4", "_c51", "_c52") \
    .withColumn("sku_idr_sku", df["_c0"].cast(IntegerType())) \
    .withColumn("sku_num_sku", df["_c1"].cast(IntegerType())) \
    .withColumn("sku_num_sku_r3", df["_c45"].cast(IntegerType())) \
    .withColumn("mdl_num_model_r3", df["_c4"].cast(IntegerType())) \
    .withColumn("sku_date_begin", df["_c51"].cast(TimestampType())) \
    .withColumn("sku_date_end", df["_c52"].cast(TimestampType())) \
    .select("sku_idr_sku", "sku_num_sku", "sku_num_sku_r3", "mdl_num_model_r3", "sku_date_begin", "sku_date_end")
# Filter
df = df.filter(col("sku_date_end") >= "2017-01-01")
df.cache()
df.createOrReplaceTempView("d_sku_h")
# list of dates
begin = date(2017, 1, 1)
base = datetime.today().date()
delta = base - begin
date_list = [base - timedelta(days=x) for x in range(0, delta.days)]
date_str = [date_obj.strftime('%Y-%m-%d') for date_obj in date_list]
dates = spark.createDataFrame(date_str, StringType()).withColumnRenamed("value", "Dates")
broadcast(dates)
dates.createOrReplaceTempView("dates")

# join to create partitions
d_sku_h = sqlContext.sql("""
SELECT DISTINCT
    d_sku_h.SKU_IDR_SKU,
    d_sku_h.SKU_NUM_SKU,
    d_sku_h.SKU_NUM_SKU_R3,
    d_sku_h.MDL_NUM_MODEL_R3,
    cast(year(dates.Dates) as string) as year_sku,
    cast(month(dates.Dates) as string) as month_sku,
    cast(day(dates.Dates) as string) as day_sku
FROM
    dates
LEFT JOIN
    d_sku_h
ON 
    dates.Dates >= d_sku_h.SKU_DATE_BEGIN 
and dates.Dates <= d_sku_h.SKU_DATE_END
""")

d_sku_h.repartition(80).write.option("compression", "snappy")\
       .mode("overwrite").format("parquet")\
       .insertInto("kylin_usb_mqb.d_sku_h")

# stopping session
spark.sparkContext.stop()
