"""SparkSession configurée pour le S3 d'Onyxia (S3A + identifiants STS temporaires) et un catalogue Iceberg.

Usage :
    from spark_session import get_spark, s3_path
    spark = get_spark("bronze-to-silver")
    df = spark.read.csv(s3_path("bronze", "fichier.csv"), header=True, schema=SCHEMA)
"""

import os

from pyspark.sql import SparkSession

# Adapter aux versions affichées par `pyspark --version` (Spark 4.1 / Scala 2.13 dans le cours).
ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"

BUCKET = os.environ.get("BUCKET") or os.environ["KUBERNETES_NAMESPACE"]
S3_PREFIX = os.environ.get("S3_PREFIX", "project")


def s3_path(layer: str, *parts: str) -> str:
    """Chemin s3a:// d'une couche medallion : s3_path("silver", "table") -> s3a://<bucket>/project/silver/table."""
    return "/".join([f"s3a://{BUCKET}", S3_PREFIX, layer, *parts])


def get_spark(app_name: str = "bigdata-project", iceberg: bool = True) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.hadoop.fs.s3a.endpoint", f"https://{os.environ['AWS_S3_ENDPOINT']}")
        .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
        .config("spark.hadoop.fs.s3a.session.token", os.environ.get("AWS_SESSION_TOKEN", ""))
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    )
    if iceberg:
        builder = (
            builder.config("spark.jars.packages", ICEBERG_PACKAGE)
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.lake.type", "hadoop")
            .config("spark.sql.catalog.lake.warehouse", f"s3a://{BUCKET}/{S3_PREFIX}/warehouse")
        )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


if __name__ == "__main__":
    # Test rapide : python src/processing/spark_session.py
    spark = get_spark("smoke-test", iceberg=False)
    print("Spark", spark.version, "| bucket", BUCKET)
    spark.range(5).show()
    spark.stop()
