from pyspark.sql import SparkSession
from os import getenv
import traceback
from dimension import DimensionProcessor
from staging import TableProcessor

project_id = getenv("GCP_PROJECT")
zone = getenv("GCP_ZONE")

if __name__ == "__main__":
    spark: SparkSession = (
        SparkSession.builder.appName("hotel processor")
        .getOrCreate()
    )

    (
        DimensionProcessor("addon", project_id, zone)
        .read_stream(spark)
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
