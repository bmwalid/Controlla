# import libraries
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import *


# init sparkConf
conf = SparkConf()
conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .set("spark.executor.cores", "1") \
    .set("spark.executor.memory", "2G") \
    .set("spark.sql.cbo.enabled", "true")

# Initialize Spark Session
spark = SparkSession.builder.appName("hierarchies").config(conf=SparkConf()).enableHiveSupport().getOrCreate()

# Initialize Spark Context
sc = spark.sparkContext

# defining HDFS path
hdfs = "hdfs:///user/hive/warehouse/"

# Defining HDFS database used
db = "kylin_usb_mqb.db/"

# defining table used
hdfs_tables = ['flat_structure/']

# loading flat structure
flat_structure = spark.read.parquet(hdfs + db + hdfs_tables[0])

flat_structure = flat_structure \
    .withColumn("FST_ARTICLE_R3", flat_structure["FST_ARTICLE_R3"].cast(IntegerType())) \
    .withColumn("FST_MODELE_R3", flat_structure["FST_MODELE_R3"].cast(IntegerType())) \
    .withColumn("ELN_NUM_ELT_NIVEAU_FAM", flat_structure["ELN_NUM_ELT_NIVEAU_FAM"].cast(IntegerType())) \
    .withColumn("ELN_NUM_ELT_NIVEAU_SSR", flat_structure["ELN_NUM_ELT_NIVEAU_SSR"].cast(IntegerType())) \
    .withColumn("ELN_NUM_ELT_NIVEAU_RAY", flat_structure["ELN_NUM_ELT_NIVEAU_RAY"].cast(IntegerType())) \
    .withColumn("ELN_NUM_ELT_NIVEAU_UNI", flat_structure["ELN_NUM_ELT_NIVEAU_UNI"].cast(IntegerType())) \
 \
# writing dmi
dmi_hierarchie_article = flat_structure \
    .filter(col("org_num_organisation_org") == 1) \
    .select("fst_article_r3", "fst_modele_r3", "eln_num_elt_niveau_fam", "eln_num_elt_niveau_ssr",
            "eln_num_elt_niveau_ray", "eln_num_elt_niveau_uni", "libelle_modele", "libelle_famille",
            "libelle_sous_rayon", "libelle_rayon", "libelle_univers") \
    .drop_duplicates()
dmi_hierarchie_article.repartition(80).write.option("compression", "snappy").mode("overwrite").format(
    "parquet").saveAsTable("kylin_usb_mqb.dmi_hierarchie_article")
dmi_hierarchie_article.select([col(c).cast("string") for c in dmi_hierarchie_article.columns]).coalesce(1).write.option(
    "sep", ";").mode("overwrite").save(
    "s3://preprod-decathlon-kylin/MyQualityBoard/lookup_tables/001-dmi_hierarchie_article_csv/", format='csv', header=True)

dmi_hierarchie_modele = flat_structure \
    .filter(col("org_num_organisation_org") == 1) \
    .select("fst_modele_r3", "eln_num_elt_niveau_fam", "eln_num_elt_niveau_ssr", "eln_num_elt_niveau_ray",
            "eln_num_elt_niveau_uni", "libelle_modele", "libelle_famille", "libelle_sous_rayon", "libelle_rayon",
            "libelle_univers") \
    .drop_duplicates()
dmi_hierarchie_modele.repartition(80).write.option("compression", "snappy").mode("overwrite").format(
    "parquet").saveAsTable("kylin_usb_mqb.dmi_hierarchie_modele")
dmi_hierarchie_modele.select([col(c).cast("string") for c in dmi_hierarchie_modele.columns]).coalesce(1).write.option(
    "sep", ";").mode("overwrite").save(
    "s3://preprod-decathlon-kylin/MyQualityBoard/lookup_tables/001-dmi_hierarchie_article_csv/", format='csv', header=True)

# writing mag
magasin_hierarchie_article = flat_structure \
    .filter(col("org_num_organisation_org") == 2) \
    .select("fst_article_r3", "fst_modele_r3", "eln_num_elt_niveau_fam", "eln_num_elt_niveau_ssr",
            "eln_num_elt_niveau_ray", "eln_num_elt_niveau_uni", "libelle_modele", "libelle_famille",
            "libelle_sous_rayon", "libelle_rayon", "libelle_univers") \
    .drop_duplicates()
magasin_hierarchie_article.repartition(80).write.option("compression", "snappy").mode("overwrite").format(
    "parquet").saveAsTable("kylin_usb_mqb.magasin_hierarchie_article")
magasin_hierarchie_article.select([col(c).cast("string") for c in magasin_hierarchie_article.columns]).coalesce(
    1).write.option("sep", ";").mode("overwrite").save(
    "s3://preprod-decathlon-kylin/MyQualityBoard/lookup_tables/001-magasin_hierarchie_article_csv/", format='csv', header=True)

magasin_hierarchie_modele = flat_structure \
    .filter(col("org_num_organisation_org") == 2) \
    .select("fst_modele_r3", "eln_num_elt_niveau_fam", "eln_num_elt_niveau_ssr", "eln_num_elt_niveau_ray",
            "eln_num_elt_niveau_uni", "libelle_modele", "libelle_famille", "libelle_sous_rayon", "libelle_rayon",
            "libelle_univers") \
    .drop_duplicates()
magasin_hierarchie_modele.repartition(80).write.option("compression", "snappy").mode("overwrite").format(
    "parquet").saveAsTable("kylin_usb_mqb.magasin_hierarchie_modele")
magasin_hierarchie_modele.select([col(c).cast("string") for c in magasin_hierarchie_modele.columns]).coalesce(
    1).write.option("sep", ";").mode("overwrite").save(
    "s3://preprod-decathlon-kylin/MyQualityBoard/lookup_tables/001-magasin_hierarchie_article_csv/", format='csv', header=True)

# writing pdt
produit_hierarchie_article = flat_structure \
    .filter(col("org_num_organisation_org") == 2) \
    .select("fst_article_r3", "fst_modele_r3", "eln_num_elt_niveau_fam", "eln_num_elt_niveau_ssr",
            "eln_num_elt_niveau_ray", "eln_num_elt_niveau_uni", "libelle_modele", "libelle_famille",
            "libelle_sous_rayon", "libelle_rayon", "libelle_univers") \
    .drop_duplicates()
produit_hierarchie_article.repartition(80).write.option("compression", "snappy").mode("overwrite").format(
    "parquet").saveAsTable("kylin_usb_mqb.produit_hierarchie_article")
produit_hierarchie_article.select([col(c).cast("string") for c in produit_hierarchie_article.columns]).coalesce(
    1).write.option("sep", ";").mode("overwrite").save(
    "s3://preprod-decathlon-kylin/MyQualityBoard/lookup_tables/001-produit_hierarchie_article_csv/", format='csv', header=True)

produit_hierarchie_modele = flat_structure \
    .filter(col("org_num_organisation_org") == 2) \
    .select("fst_modele_r3", "eln_num_elt_niveau_fam", "eln_num_elt_niveau_ssr", "eln_num_elt_niveau_ray",
            "eln_num_elt_niveau_uni", "libelle_modele", "libelle_famille", "libelle_sous_rayon", "libelle_rayon",
            "libelle_univers") \
    .drop_duplicates()
produit_hierarchie_modele.repartition(80).write.option("compression", "snappy").mode("overwrite").format(
    "parquet").saveAsTable("kylin_usb_mqb.produit_hierarchie_modele")
produit_hierarchie_modele.select([col(c).cast("string") for c in produit_hierarchie_modele.columns]).coalesce(
    1).write.option("sep", ";").mode("overwrite").save(
    "s3://preprod-decathlon-kylin/MyQualityBoard/lookup_tables/001-produit_hierarchie_article_csv/", format='csv', header=True)

# writing nat
nature_produit_hierarchie_article = flat_structure \
    .filter(col("org_num_organisation_org") == 2) \
    .select("fst_article_r3", "fst_modele_r3", "eln_num_elt_niveau_fam", "eln_num_elt_niveau_ssr",
            "eln_num_elt_niveau_ray", "eln_num_elt_niveau_uni", "libelle_modele", "libelle_famille",
            "libelle_sous_rayon", "libelle_rayon", "libelle_univers") \
    .drop_duplicates()
nature_produit_hierarchie_article.repartition(80).write.option("compression", "snappy").mode("overwrite").format(
    "parquet").saveAsTable("kylin_usb_mqb.nature_produit_hierarchie_article")
nature_produit_hierarchie_article.select(
    [col(c).cast("string") for c in nature_produit_hierarchie_article.columns]).coalesce(1).write.option("sep",
                                                                                                         ";").mode(
    "overwrite").save("s3://preprod-decathlon-kylin/MyQualityBoard/lookup_tables/001-nature_produit_hierarchie_article_csv/",
                      format='csv', header=True)

nature_produit_hierarchie_modele = flat_structure \
    .filter(col("org_num_organisation_org") == 2) \
    .select("fst_modele_r3", "eln_num_elt_niveau_fam", "eln_num_elt_niveau_ssr", "eln_num_elt_niveau_ray",
            "eln_num_elt_niveau_uni", "libelle_modele", "libelle_famille", "libelle_sous_rayon", "libelle_rayon",
            "libelle_univers") \
    .drop_duplicates()
nature_produit_hierarchie_modele.repartition(80).write.option("compression", "snappy").mode("overwrite").format(
    "parquet").saveAsTable("kylin_usb_mqb.nature_produit_hierarchie_modele")
nature_produit_hierarchie_modele.select(
    [col(c).cast("string") for c in nature_produit_hierarchie_modele.columns]).coalesce(1).write.option("sep",
                                                                                                        ";").mode(
    "overwrite").save("s3://preprod-decathlon-kylin/MyQualityBoard/lookup_tables/001-nature_produit_hierarchie_article_csv/",
                      format='csv', header=True)

# stopping session
spark.sparkContext.stop()
