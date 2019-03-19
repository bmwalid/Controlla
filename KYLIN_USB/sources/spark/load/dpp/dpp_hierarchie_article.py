# Import libraries
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import date_format
from collections import namedtuple
from pyspark.sql import *

# Defining Spark Session
spark = SparkSession.builder.appName("dpp_hierarchie_article").enableHiveSupport().getOrCreate()
sc = spark.sparkContext
sqlContext = HiveContext(sc)

# Defining database used
sqlContext.sql("use kylin_usb_mqb")

path = "s3://decathlon-cds/cds_supply/material_dpp_hierarchy/*.gz"

cs0 = spark.read.option("header", "false").option("delimiter", "|").csv(path)
cs0.createOrReplaceTempView("tt")

Mapping = namedtuple("Mapping", ["nom", "format", "position"])
l1 = [("_c0", "Integer", 1), ("_c1", "Integer", 2), ("_c2", "Integer", 3), ("_c3", "String", 4), ("_c4", "Integer", 5),
      ("_c5", "String", 6), ("_c6", "Integer", 7), ("_c7", "String", 8), ("_c8", "Integer", 9), ("_c9", "String", 10),
      ("_c10", "Integer", 11), ("_c11", "Date", 12)]

l1 = [Mapping(*t) for t in l1];
list_col = cs0.columns


# print(list_col)


def find_pos(pos, collection):
    res = False
    for i, t in enumerate(collection):
        if t.position == pos:
            res = t
            break
    return res


def find_name(name, collection):
    res = False
    for i, t in enumerate(collection):
        if t.name == name:
            res = t
            break
    return res


def list_format():
    pattern = {"Date": "TO_DATE(CAST(UNIX_TIMESTAMP({0}, 'yyyy-mm-dd') AS TIMESTAMP)) as {0}",
               "Integer": "CAST({0} AS INTEGER) as {0}", "Float": "CAST({0} AS FLOAT) as {0}",
               "Timestamp": "CAST(UNIX_TIMESTAMP({0},'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP ) as {0}"}
    for cpt in range(1, len(list_col) + 1):
        col0 = find_pos(cpt, l1)
        col0 = col0 if col0 else Mapping("0", "NULL", 0)
        frmt = col0.format
        cc = col0.nom
        instr00 = pattern[frmt].format(cc) if frmt in pattern else cc
        yield instr00


sqlInstruction = "SELECT DISTINCT " + " , ".join(list_format()) + " FROM tt WHERE _c0 is not null and _c1 is not null"
# print(sqlInstruction)

tableFinale = spark.sql(sqlInstruction)
tableFinale = tableFinale.withColumnRenamed("_c0", "vendor").withColumnRenamed("_c1", "materdpp").withColumnRenamed(
    "_c2", "subdpdpp").withColumnRenamed("_c3", "subdplib").withColumnRenamed("_c4", "sectordpp").withColumnRenamed(
    "_c5", "sectorlib").withColumnRenamed("_c6", "familydpp").withColumnRenamed("_c7", "familylib").withColumnRenamed(
    "_c8", "deptdpp").withColumnRenamed("_c9", "deptlib").withColumnRenamed("_c10", "rt_confmat").withColumnRenamed(
    "_c11", "dateint")

tableFinale = tableFinale.na.drop(subset=["vendor"])
tableFinale = tableFinale.na.drop(subset=["materdpp"])

tableFinale.createOrReplaceTempView("tt")
maximum_dateint = sqlContext.sql("select max(dateint) from tt")

maximum_date_dpp = maximum_dateint.collect()[0].asDict()['max(dateint)'].isoformat()
tableFinale = sqlContext.sql("select * from tt where dateint='" + maximum_date_dpp + "'")
tableFinale.registerTempTable("DPP")

dmi_libelle = sqlContext.sql("select distinct libelle_modele,fst_modele_r3 from dmi_hierarchie_article")
dmi_libelle.registerTempTable("DMI")

tableFinale = sqlContext.sql(
    "select DPP.*,DMI.libelle_modele from DPP left join DMI on DMI.fst_modele_r3=DPP.rt_confmat")

tableFinale.repartition(80).write.option("compression", "snappy").mode("overwrite").format("parquet").saveAsTable(
    "kylin_usb_mqb.dpp_hierarchie_article")
tableFinale.select([col(c).cast("string") for c in tableFinale.columns]).coalesce(1).write.option("sep", ";").mode(
    "overwrite").save("s3://preprod-decathlon-kylin/MyQualityBoard/lookup_tables/002-dpp_hierarchie_article_csv/", format='csv',
                      header=True)
