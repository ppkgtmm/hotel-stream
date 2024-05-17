from pyspark.sql import DataFrame
from pyspark.sql.functions import col, isnotnull, isnull
from common import clean_map, dim_upsert_query, dim_delete_query
from processor import Processor


class DimensionProcessor(Processor):

    def __init__(
        self,
        dimension: str,
        username: str,
        password: str,
        host: str,
        database: str,
        temp_dir: str,
        aws_region: str,
    ):
        super().__init__(
            dimension, username, password, host, database, temp_dir, aws_region
        )

    def __upsert_records(self, df: DataFrame, batch_id: int):
        if df.count() == 0:
            return
        staging_table = "staging.upsert_" + self.table_name
        dimension_table = "dim_" + self.table_name
        self.stage_records(df, staging_table)
        self.engine.execute(
            dim_upsert_query.format(dim=dimension_table, stage=staging_table)
        )
        self.stage_records(df, dimension_table, "append")

    def __delete_records(self, df: DataFrame, batch_id: int):
        if df.count() == 0:
            return
        staging_table = "staging.delete_" + self.table_name
        dimension_table = "dim_" + self.table_name
        self.stage_records(df, staging_table)
        self.engine.execute(
            dim_delete_query.format(dim=dimension_table, stage=staging_table)
        )

    def load_stream(self):
        (
            self.data.filter(isnotnull(col("after")))
            .select("after.*")
            .selectExpr(*clean_map[self.table_name])
            .writeStream.foreachBatch(self.__upsert_records)
            .start()
        )
        (
            self.data.filter(isnotnull(col("before")) & isnull(col("after")))
            .select("before.*")
            .selectExpr(*clean_map[self.table_name])
            .writeStream.foreachBatch(self.__delete_records)
            .start()
        )
