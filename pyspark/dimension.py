from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, isnotnull, isnull, row_number
from common import clean_map, dim_upsert_query, dim_delete_query, maxid_query
from processor import Processor


class DimensionProcessor(Processor):

    def __init__(self, dimension: str, project_id: str, zone: str):
        super().__init__(dimension, project_id, zone)

    def __upsert_records(self, df: DataFrame, batch_id: int):
        if df.count() == 0:
            return
        staging_table = "staging.upsert_" + self.table_name
        dimension_table = "dim_" + self.table_name
        self.stage_records(df, staging_table)
        self.execute_query(
            dim_upsert_query.format(dim=dimension_table, stage=staging_table)
        )
        self.stage_records(df, dimension_table, "append")

    def __delete_records(self, df: DataFrame, batch_id: int):
        if df.count() == 0:
            return
        staging_table = "staging.delete_" + self.table_name
        dimension_table = "dim_" + self.table_name
        self.stage_records(df, staging_table)
        self.execute_query(
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
