from pyspark.sql import DataFrame
from pyspark.sql.functions import col, isnull, from_json
from common import (
    dim_clean_map,
    dim_schema_map,
    dim_upsert_query,
    dim_delete_query,
    maxid_query,
)
from processor import Processor


class DimensionProcessor(Processor):

    def __init__(self, dimension: str, project_id: str, zone: str):
        super().__init__(dimension, project_id, zone)

    def __upsert_records(self, df: DataFrame, batch_id: int):
        if df.count() == 0:
            return
        staging_table = self.staging_dataset + ".upsert_" + self.table_name
        dimension_table = self.dest_dataset + ".dim_" + self.table_name
        self.stage_records(df, staging_table)
        self.execute_query(
            dim_upsert_query.format(
                dim=dimension_table,
                stage=staging_table,
                maxid=(
                    self.execute_query(maxid_query.format(dim=dimension_table))
                    .to_dataframe()
                    .iloc[0, 0]
                ),
                columns=", ".join(df.columns),
            )
        )

    def __delete_records(self, df: DataFrame, batch_id: int):
        if df.count() == 0:
            return
        staging_table = self.staging_dataset + ".delete_" + self.table_name
        dimension_table = self.dest_dataset + ".dim_" + self.table_name
        self.stage_records(df, staging_table)
        self.execute_query(
            dim_delete_query.format(dim=dimension_table, stage=staging_table)
        )

    def load_stream(self):
        (
            self.data.select(
                from_json(col("before"), dim_schema_map[self.table_name]).alias(
                    "before"
                ),
                from_json(col("after"), dim_schema_map[self.table_name]).alias("after"),
            )
            .filter(col("after").isNotNull())
            .select("after.*")
            .selectExpr(*dim_clean_map[self.table_name])
            .writeStream.foreachBatch(self.__upsert_records)
            .start()
        )
        (
            self.data.select(
                from_json(col("before"), dim_schema_map[self.table_name]).alias(
                    "before"
                ),
                from_json(col("after"), dim_schema_map[self.table_name]).alias("after"),
            )
            .filter(col("before").isNotNull() & isnull(col("after")))
            .select("before.*")
            .selectExpr(*dim_clean_map[self.table_name])
            .writeStream.foreachBatch(self.__delete_records)
            .start()
        )
