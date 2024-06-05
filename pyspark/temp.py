from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json
from common import temp_clean_map, temp_schema_map
from processor import Processor


class TempTableProcessor(Processor):

    def __insert_records(self, df: DataFrame, batch_id: int):
        if df.count() == 0:
            return
        staging_table = self.staging_dataset + "." + self.table_name
        self.stage_records(df, staging_table, mode="append")

    def load_stream(self):
        (
            self.data.select(
                from_json(col("before"), temp_schema_map[self.table_name]).alias(
                    "before"
                ),
                from_json(col("after"), temp_schema_map[self.table_name]).alias(
                    "after"
                ),
            )
            .filter(col("after").isNotNull())
            .select("after.*")
            .selectExpr(*temp_clean_map[self.table_name])
            .writeStream.foreachBatch(self.__insert_records)
            .start()
        )
