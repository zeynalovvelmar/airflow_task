import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

def main():
    parser = argparse.ArgumentParser(description="Postgres-dən oxu, Transformasiya et və MinIO-ya yüklə")
    parser.add_argument("--postgres_table", required=True, help="Oxunacaq PostgreSQL cədvəlinin adı")
    parser.add_argument("--s3_path", required=True, help="Məlumatın yazılacağı MinIO S3 yolu")
    parser.add_argument("--etl_date", required=True, help="Airflow kontekstindən gələn ETL icra tarixi")
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName(f"ETL_Process_{args.postgres_table}")
        .config("spark.jars", "/opt/airflow/postgresql-42.7.2.jar")
        .config(
            "spark.jars.packages", 
            "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
        )
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

    jdbc_url = "jdbc:postgresql://postgres:5432/airflow"

    df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", args.postgres_table) \
        .option("user", "airflow") \
        .option("password", "airflow") \
        .option("driver", "org.postgresql.Driver") \
        .load()

    df_transformed = df.withColumn("etl_date", lit(args.etl_date))

    df_transformed.write \
        .format("parquet") \
        .mode("overwrite") \
        .save(args.s3_path)

    spark.stop()

if __name__ == "__main__":
    main()