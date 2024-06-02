from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, get_json_object, from_json
from common import schema_map
import redshift_connector


class Processor:
    __max_messages = 100

    def __init__(self, table_name: str, project_id: str, zone: str):
        self.table_name = table_name
        self.project_id = project_id
        self.zone = zone

    def read_stream(self, spark: SparkSession):
        subscription = f"projects/{self.project_id}/locations/{self.zone}/subscriptions/{self.table_name}"
        self.data = (
            spark.readStream.format("pubsublite")
            .option("pubsublite.subscription", subscription)
            .option(
                "pubsublite.flowcontrol.maxmessagesperbatch", Processor.__max_messages
            )
            .load()
        )
        return self

    def process_stream(self):
        self.data = (
            self.data.select(col("data").cast("string").alias("data"))
            .withColumn("before", get_json_object(col("data"), "$.payload.before"))
            .withColumn("after", get_json_object(col("data"), "$.payload.after"))
            .select(
                from_json(col("before"), schema_map[self.table_name]).alias("before"),
                from_json(col("after"), schema_map[self.table_name]).alias("after"),
            )
        )
        return self

    def stage_records(self, df: DataFrame, table_name: str, mode="overwrite"):
        (
            df.write.format("io.github.spark_redshift_community.spark.redshift")
            .option("url", self.__jdbc_url)
            .option("dbtable", table_name)
            .option("tempdir", self.temp_dir)
            .option("tempdir_region", self.aws_region)
            .option("forward_spark_s3_credentials", "true")
            .mode(mode)
            .save()
        )

    def execute_query(self, query: str):
        # redshift_connector.paramstyle = "named"
        with redshift_connector.connect(**self.redshift_parameters) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
            conn.commit()
