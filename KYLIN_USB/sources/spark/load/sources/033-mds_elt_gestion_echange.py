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
    .set("spark.executor.cores", "1") \
    .set("spark.executor.memory", "1G")

# Initialize Spark Session
spark = SparkSession.builder.appName("033-mds_elt_gestion_echange").config(
    conf=SparkConf()).enableHiveSupport().getOrCreate()
sc = spark.sparkContext
sqlContext = HiveContext(sc)
sqlContext.setConf("hive.exec.dynamic.partition", "true")
sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

# Data path
path = "s3://decathlon-ods/ods/mds_elt_gestion_echange/*.gz"

# From gz files on S3 to Spark Dataframe
df = spark.read.option("header", "false").option("delimiter", "|").csv(path)

# select
df = df.select("_c0", "_c11", "_c12") \
    .withColumn("elg_num_elt_gestion_elg", df["_c0"].cast(IntegerType())) \
    .withColumn("eta_num_etape_eta", df["_c11"].cast(StringType())) \
    .withColumn("ege_date_maj_etape", df["_c12"].cast(DateType())) \
    .select("elg_num_elt_gestion_elg", "eta_num_etape_eta", "ege_date_maj_etape")\
    .filter(~col("ege_date_maj_etape").isNull())\
    .drop_duplicates()
df.cache()
df.createOrReplaceTempView("mge")
# list of dates
begin = date(2017, 1, 1)
base = datetime.today().date()
delta = base - begin
date_list = [base - timedelta(days=x) for x in range(0, delta.days)]
date_str = [date_obj.strftime('%Y-%m-%d') for date_obj in date_list]
dates = spark.createDataFrame(date_str, StringType()).withColumnRenamed("value", "Dates")
broadcast(dates)
dates.createOrReplaceTempView("dates")

mge = sqlContext.sql("""
SELECT DISTINCT
    df.elg_num_elt_gestion_elg,
    df.eta_num_etape_eta,
    df.year_mge,
    df.month_mge,
    df.day_mge
FROM
    (
    SELECT DISTINCT
        mge.elg_num_elt_gestion_elg,
        mge.eta_num_etape_eta,
        mge.ege_date_maj_etape,
        cast(year(dates.Dates) as string) as year_mge,
        cast(month(dates.Dates) as string) as month_mge,
        cast(day(dates.Dates) as string) as day_mge,
        row_number() over (partition by elg_num_elt_gestion_elg order by ege_date_maj_etape desc) as RN 
    FROM
        dates
    LEFT JOIN
        mge 
    ON 
        mge.ege_date_maj_etape <= dates.Dates
    ) df 
WHERE RN = 1
""")

mge.repartition(80).write.option("compression", "snappy").mode("overwrite").format("parquet").insertInto(
    "kylin_usb_mqb.mds_elt_gestion_echange")

# stopping session
spark.sparkContext.stop()
