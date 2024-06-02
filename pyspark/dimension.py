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
                columns=", ".join([col for col in df.columns if col != "rownum"]),
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
            self.data.filter(isnotnull(col("after")))
            .select("after.*")
            .selectExpr(*clean_map[self.table_name])
            .withColumn("rownum", row_number().over(Window.orderBy("effective_from")))
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
