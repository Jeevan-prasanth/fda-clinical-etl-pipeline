from pyspark.sql import SparkSession

def get_spark():
    spark = (
        SparkSession.builder
        .appName("hudi-local")
        .config("spark.jars", r"D:\DE_Assignment\hudi\hudi.jar")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.hive.convertMetastoreParquet", "false")
        .getOrCreate()
    )
    return spark
