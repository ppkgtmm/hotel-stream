from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, get_json_object
from google.cloud.bigquery import Client


class Processor:
    __max_messages = 100

    def __init__(self, table_name: str, project_id: str, zone: str):
        self.table_name = table_name
        self.project_id = project_id
        self.zone = zone
        self.staging_dataset = "staging"
        self.dest_dataset = "warehouse"

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
        )
        return self

    def stage_records(self, df: DataFrame, table_name: str, mode="overwrite"):
        (df.write.format("bigquery").option("table", table_name).mode(mode).save())

    def execute_query(self, query: str):
        with Client() as bq_client:
            return bq_client.query(query).result()
