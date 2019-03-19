# import Libraries
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# init sparkConf
conf = SparkConf()
conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .set("spark.executor.cores", "1") \
    .set("spark.executor.memory", "1G")
# Initialize Spark Session
spark = SparkSession.builder.appName("014-opv_review_review").config(conf=SparkConf()).enableHiveSupport().getOrCreate()

# Data path
path = "s3://decathlon-ods/openvoice/review__review/*.gz"

# From gz files on S3 to Spark Dataframe
df = spark.read.option("header", "false").option("delimiter", "|").csv(path)

# write to hdfs
df = df.select("_c47", "_c2", "_c0", "_c11", "_c17", "_c22", "_c59", "_c5") \
    .withColumn("product_reference_id", df["_c47"].cast(T.IntegerType())) \
    .withColumn("country_reference", df["_c2"].cast(T.StringType())) \
    .withColumn("id", df["_c0"].cast(T.IntegerType())) \
    .withColumn("note", df["_c11"].cast(T.IntegerType())) \
    .withColumn("collaborator", df["_c17"].cast(T.IntegerType())) \
    .withColumn("day_published_at", df["_c22"].cast(T.DateType())) \
    .withColumn("updated_at", df["_c59"].cast(T.DateType())) \
    .withColumn("unpublished_at", df["_c5"].cast(T.DateType())) \
    .withColumn("week_published_at", F.weekofyear(df["_c22"].cast(T.DateType()))) \
    .withColumn("month_published_at", F.month(df["_c22"].cast(T.DateType()))) \
    .withColumn("year_published_at", F.year(df["_c22"].cast(T.DateType()))) \
    .select("product_reference_id", "country_reference", "id", "note", "collaborator", "day_published_at", "updated_at",
            "unpublished_at", "week_published_at", "month_published_at", "year_published_at") \
    .filter(
    (F.col("year_published_at") >= 2017) & (~F.col("day_published_at").isNull()) & (F.col("unpublished_at").isNull())) \
    .withColumn("row", F.row_number().over(Window.partitionBy("id").orderBy(F.desc("updated_at")))) \
    .filter((F.col("row") == 1) & (F.col("note") >= 0) & (F.col("note") <= 5)) \

# Data path
path = "s3://decathlon-ods/openvoice/catalog__product/*.gz"
# From gz files on S3 to Spark Dataframe
df2 = spark.read.option("header", "false").option("delimiter", "|").csv(path)

# write to hdfs
df2 = df2.select("_c0", "_c5") \
    .withColumn("id", df2["_c0"].cast(T.IntegerType())) \
    .withColumn("code", df2["_c5"].cast(T.StringType())) \
    .select("id", "code").filter((F.length(F.col("code")) <= 8)) \
    .drop_duplicates()

df = df.join(df2, how='left', on=[df.product_reference_id == df2.id])\
       .select(df.product_reference_id,df.country_reference, df.id, df.note,
                 df.collaborator, df.day_published_at,
                 df.updated_at, df.unpublished_at,
                 df.week_published_at,
                 df.month_published_at,
                 df.year_published_at,
                 df2.code).drop_duplicates()\
        .repartition(80)


df.write.option("compression", "snappy").mode("overwrite").format("parquet").saveAsTable("kylin_usb_mqb.opv")
# stopping session
spark.sparkContext.stop()
