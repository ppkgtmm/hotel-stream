from pyspark.sql import SparkSession
from os import getenv
import traceback
from dimension import DimensionProcessor
from staging import TableProcessor
from temp import TempTableProcessor

project_id = getenv("GCP_PROJECT")
zone = getenv("GCP_ZONE")

if __name__ == "__main__":
    spark: SparkSession = (
        SparkSession.builder.appName("hotel processor")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        .config("spark.driver.cores", "1")
        .config("spark.executor.cores", "1")
        .getOrCreate()
    )
    spark.conf.set("temporaryGcsBucket", getenv("GCS_BUCKET"))
    spark._jsc.hadoopConfiguration().set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')
    spark._jsc.hadoopConfiguration().set("fs.gs.project.id", project_id)

    (
        DimensionProcessor("addon", project_id, zone)
        .read_stream(spark)
        .process_stream()
        .load_stream()
    )

    (
        DimensionProcessor("roomtype", project_id, zone)
        .read_stream(spark)
        .process_stream()
        .load_stream()
    )

    (
        DimensionProcessor("guest", project_id, zone)
        .read_stream(spark)
        .process_stream()
        .load_stream()
    )

    (
        TempTableProcessor("guest", project_id, zone)
        .read_stream(spark)
        .process_stream()
        .load_stream()
    )

    (
        DimensionProcessor("location", project_id, zone)
        .read_stream(spark)
        .process_stream()
        .load_stream()
    )

    (
        TempTableProcessor("room", project_id, zone)
        .read_stream(spark)
        .process_stream()
        .load_stream()
    )

    (
        TableProcessor("booking", project_id, zone)
        .read_stream(spark)
        .process_stream()
        .load_stream()
    )

    (
        TableProcessor("booking_room", project_id, zone)
        .read_stream(spark)
        .process_stream()
        .load_stream()
    )

    (
        TableProcessor("booking_addon", project_id, zone)
        .read_stream(spark)
        .process_stream()
        .load_stream()
    )

    try:
        spark.streams.awaitAnyTermination()
    except Exception as e:
        traceback.print_exc()
