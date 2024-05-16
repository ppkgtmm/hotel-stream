from pyspark.sql import DataFrame
from pyspark.sql.functions import col, isnotnull, isnull, lit
from common import clean_map, get_upsert_query, stg_delete_query
from processor import Processor


class TableProcessor(Processor):

    def upsert_records(self, df: DataFrame, batch_id: int):
        temp_table = "staging.upsert_" + self.table_name
        staging_table = "staging." + self.table_name
        df = df.withColumn("is_deleted", lit(False))
        self.__stage_records(df, temp_table)
        self.__engine.execute(get_upsert_query(staging_table, temp_table, df.columns))

    def delete_records(self, df: DataFrame, batch_id: int):
        temp_table = "staging.delete_" + self.table_name
        staging_table = "staging." + self.table_name
        df = df.withColumn("is_deleted", lit(True))
        self.__stage_records(df, temp_table)
        self.__engine.execute(
            stg_delete_query.format(stg=staging_table, temp=temp_table)
        )

    def load_stream(self):
        (
            self.data.filter(isnotnull(col("after")))
            .select("after.*")
            .selectExpr(*clean_map[self.table_name])
            .writeStream.foreachBatch(self.upsert_records)
            .start()
        )
        (
            self.data.filter(isnotnull(col("before")) & isnull(col("after")))
            .select("before.*")
            .selectExpr(*clean_map[self.table_name])
            .writeStream.foreachBatch(self.delete_records)
            .start()
        )
