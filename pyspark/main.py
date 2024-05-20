from pyspark.sql import SparkSession
from os import getenv, path
import traceback
from dimension import DimensionProcessor
from staging import TableProcessor

broker = getenv("KAFKA_SERVER")
username, password = getenv("DWH_USER"), getenv("DWH_PASSWORD")
host, database = getenv("DWH_HOST"), getenv("DWH_NAME")
topic_prefix = getenv("DB_NAME") + "." + "public"
s3_temp_dir = "s3a://" + path.join(getenv("S3_BUCKET"), "temp/data/")
aws_region = getenv("AWS_REGION")

if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("hotel processor")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.github.spark-redshift-community:spark-redshift_2.12:6.2.0-spark_3.5,com.amazon.redshift:redshift-jdbc42:2.1.0.26,com.amazonaws:aws-java-sdk-core:1.12.23,com.amazonaws:aws-java-sdk-redshift:1.12.23,com.amazonaws:aws-java-sdk-s3:1.12.23,com.amazonaws:aws-java-sdk-dynamodb:1.12.23,org.apache.spark:spark-avro_2.12:3.5.1",
        )  # cr. https://stackoverflow.com/questions/54285151/kafka-structured-streaming-kafkasourceprovider-could-not-be-instantiated
        .getOrCreate()
    )
    # org.apache.hadoop:hadoop-aws:3.2.2,com.amazonaws:aws-java-sdk-bundle:1.12.722

    # cr. https://mageswaran1989.medium.com/2022-no-filesystem-for-scheme-s3-cbd72c99a50c
    spark._jsc.hadoopConfiguration().set(
        "fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.InstanceProfileCredentialsProvider,com.amazonaws.auth.profile.ProfileCredentialsProvider",
    )

    (
        DimensionProcessor(
            "addon", username, password, host, database, s3_temp_dir, aws_region
        )
        .read_stream(spark, broker, topic_prefix)
        .process_stream()
        .load_stream()
    )

    (
        DimensionProcessor(
            "roomtype", username, password, host, database, s3_temp_dir, aws_region
        )
        .read_stream(spark, broker, topic_prefix)
        .process_stream()
        .load_stream()
    )

    (
        DimensionProcessor(
            "guest", username, password, host, database, s3_temp_dir, aws_region
        )
        .read_stream(spark, broker, topic_prefix)
        .process_stream()
        .load_stream()
    )

    (
        DimensionProcessor(
            "location", username, password, host, database, s3_temp_dir, aws_region
        )
        .read_stream(spark, broker, topic_prefix)
        .process_stream()
        .load_stream()
    )

    (
        TableProcessor(
            "room", username, password, host, database, s3_temp_dir, aws_region
        )
        .read_stream(spark, broker, topic_prefix)
        .process_stream()
        .load_stream()
    )

    (
        TableProcessor(
            "booking", username, password, host, database, s3_temp_dir, aws_region
        )
        .read_stream(spark, broker, topic_prefix)
        .process_stream()
        .load_stream()
    )

    (
        TableProcessor(
            "booking_room", username, password, host, database, s3_temp_dir, aws_region
        )
        .read_stream(spark, broker, topic_prefix)
        .process_stream()
        .load_stream()
    )

    (
        TableProcessor(
            "booking_addon", username, password, host, database, s3_temp_dir, aws_region
        )
        .read_stream(spark, broker, topic_prefix)
        .process_stream()
        .load_stream()
    )

    try:
        spark.streams.awaitAnyTermination()
    except Exception as e:
        traceback.print_exc()
