from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, get_json_object, from_json
from common import schema_map
import redshift_connector


class Processor:
    __max_offsets = 150

    def __init__(
        self,
        table_name: str,
        username: str,
        password: str,
        host: str,
        database: str,
        temp_dir: str,
        aws_region: str,
    ):
        self.table_name = table_name
        self.redshift_parameters = {
            "user": username,
            "password": password,
            "database": database,
            "host": host.split(":")[0],
            "port": host.split(":")[-1],
        }
        self.__jdbc_url = (
            f"jdbc:redshift://{host}/{database}?user={username}&password={password}"
        )
        self.temp_dir = temp_dir
        self.aws_region = aws_region

    def read_stream(self, spark: SparkSession, kafka_server: str, topic_prefix: str):
        self.data = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", kafka_server)
            .option("subscribe", topic_prefix + "." + self.table_name)
            .option("startingOffsets", "earliest")
            .option("maxOffsetsPerTrigger", Processor.__max_offsets)
            .load()
        )
        return self

    def process_stream(self):
        self.data = (
            self.data.select(col("value").cast("string").alias("value"))
            .withColumn("before", get_json_object(col("value"), "$.payload.before"))
            .withColumn("after", get_json_object(col("value"), "$.payload.after"))
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
