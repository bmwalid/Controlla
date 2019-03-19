# import Libraries
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import *
from pyspark.sql.functions import *

# init sparkConf
conf = SparkConf()
conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .set("spark.executor.cores", "1") \
    .set("spark.executor.memory", "1G")

# Initialize Spark Session
spark = SparkSession.builder.appName("004-stc_articles_defectueux")\
                    .config(conf=SparkConf()).enableHiveSupport().getOrCreate()

# Data path
path = "s3://decathlon-ods/ods_retail/stc_article_defectueux/*.gz"

# From gz files on S3 to Spark Dataframe
df = spark.read.option("header", "false").option("delimiter", "|").csv(path)

# write to hdfs
df.select("_c23", "_c1", "_c21", "_c3", "_c18", "_c20", "_c11", "_c13", "_c26", "_c10", "_c39") \
    .withColumn("year_tdt_date_transaction", year(df["_c23"].cast(TimestampType()))) \
    .withColumn("month_tdt_date_transaction", month(df["_c23"].cast(TimestampType()))) \
    .withColumn("week_tdt_date_transaction", weekofyear(df["_c23"].cast(TimestampType()))) \
    .withColumn("def_date_validation", df["_c23"].cast(DateType())) \
    .withColumn("def_ident_echange", df["_c1"].cast(StringType())) \
    .withColumn("tir_num_tiers_tir", df["_c3"].cast(StringType())) \
    .withColumn("dft_code_dft", df["_c21"].cast(IntegerType())) \
    .withColumn("def_type", df["_c18"].cast(IntegerType())) \
    .withColumn("loc_code_loc", df["_c20"].cast(IntegerType())) \
    .withColumn("def_is_decathlon", df["_c11"].cast(IntegerType())) \
    .withColumn("def_quantite", df["_c13"].cast(FloatType())) \
    .withColumn("def_code_destination", df["_c26"].cast(StringType())) \
    .withColumn("def_code_article_ext", df["_c10"].cast(IntegerType())) \
    .withColumn("def_date_achat", df["_c39"].cast(DateType())) \
    .select("year_tdt_date_transaction", "month_tdt_date_transaction", "week_tdt_date_transaction",
            "def_date_validation", "def_ident_echange", "tir_num_tiers_tir", "dft_code_dft", "def_type", "loc_code_loc",
            "def_is_decathlon", "def_quantite", "def_code_destination", "def_code_article_ext", "def_date_achat") \
    .repartition(80).write.option("compression", "snappy").mode("overwrite").format("parquet").saveAsTable(
    "kylin_usb_mqb.stc_articles_defectueux")

# Data path
path = "s3://decathlon-cds/cds_supply/defect_histo_cds/defect_agg_bw/*.gz"

# From gz files on S3 to Spark Dataframe
df = spark.read.option("header", "false").option("delimiter", "|").csv(path)

# write to hdfs
df.select("_c23", "_c1", "_c21", "_c3", "_c18", "_c20", "_c11", "_c13", "_c26", "_c10", "_c39") \
    .withColumn("year_tdt_date_transaction", year(df["_c23"].cast(TimestampType()))) \
    .withColumn("month_tdt_date_transaction", month(df["_c23"].cast(TimestampType()))) \
    .withColumn("week_tdt_date_transaction", weekofyear(df["_c23"].cast(TimestampType()))) \
    .withColumn("def_date_validation", df["_c23"].cast(DateType())) \
    .withColumn("def_ident_echange", df["_c1"].cast(StringType())) \
    .withColumn("tir_num_tiers_tir", df["_c3"].cast(StringType())) \
    .withColumn("dft_code_dft", df["_c21"].cast(IntegerType())) \
    .withColumn("def_type", df["_c18"].cast(IntegerType())) \
    .withColumn("loc_code_loc", df["_c20"].cast(IntegerType())) \
    .withColumn("def_is_decathlon", df["_c11"].cast(IntegerType())) \
    .withColumn("def_quantite", df["_c13"].cast(FloatType())) \
    .withColumn("def_code_destination", df["_c26"].cast(StringType())) \
    .withColumn("def_code_article_ext", df["_c10"].cast(IntegerType())) \
    .withColumn("def_date_achat", df["_c39"].cast(DateType())) \
    .select("year_tdt_date_transaction", "month_tdt_date_transaction", "week_tdt_date_transaction",
            "def_date_validation", "def_ident_echange", "tir_num_tiers_tir", "dft_code_dft", "def_type", "loc_code_loc",
            "def_is_decathlon", "def_quantite", "def_code_destination", "def_code_article_ext", "def_date_achat") \
    .repartition(80).write.option("compression", "snappy").mode("overwrite").format("parquet").saveAsTable(
    "kylin_usb_mqb.stc_articles_defectueux_histo")
# stopping session
spark.sparkContext.stop()
