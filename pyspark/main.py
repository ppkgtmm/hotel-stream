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
aws_region = getenv("REGION")

if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("hotel processor")
        # cr. https://stackoverflow.com/questions/54285151/kafka-structured-streaming-kafkasourceprovider-could-not-be-instantiated
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
