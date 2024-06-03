from pyspark.sql import DataFrame
from pyspark.sql.functions import col, isnotnull, isnull, lit, from_json
from common import stg_clean_map, stg_schema_map, get_upsert_query, stg_delete_query
from processor import Processor


class TableProcessor(Processor):

    def __upsert_records(self, df: DataFrame, batch_id: int):
        if df.count() == 0:
            return
        temp_table = self.staging_dataset + ".upsert_" + self.table_name
        staging_table = self.staging_dataset + "." + self.table_name
        df = df.withColumn("is_deleted", lit(False))
        self.stage_records(df, temp_table)
        self.execute_query(get_upsert_query(staging_table, temp_table, df.columns))

    def __delete_records(self, df: DataFrame, batch_id: int):
        if df.count() == 0:
            return
        temp_table = self.staging_dataset + ".delete_" + self.table_name
        staging_table = self.staging_dataset + "." + self.table_name
        df = df.withColumn("is_deleted", lit(True))
        self.stage_records(df, temp_table)
        self.execute_query(stg_delete_query.format(stg=staging_table, temp=temp_table))

    def load_stream(self):
        (
            self.data.select(
                from_json(col("before"), stg_schema_map[self.table_name]).alias(
                    "before"
                ),
                from_json(col("after"), stg_schema_map[self.table_name]).alias("after"),
            )
            .filter(isnotnull(col("after")))
            .select("after.*")
            .selectExpr(*stg_clean_map[self.table_name])
            .writeStream.foreachBatch(self.__upsert_records)
            .start()
        )
        (
            self.data.select(
                from_json(col("before"), stg_schema_map[self.table_name]).alias(
                    "before"
                ),
                from_json(col("after"), stg_schema_map[self.table_name]).alias("after"),
            )
            .filter(isnotnull(col("before")) & isnull(col("after")))
            .select("before.*")
            .selectExpr(*stg_clean_map[self.table_name])
            .writeStream.foreachBatch(self.__delete_records)
            .start()
        )
