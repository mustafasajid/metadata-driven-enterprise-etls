from pyspark.sql import SparkSession, DataFrame

class MetadataManager:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def load_connection_metadata(self) -> DataFrame:
        return self.spark.table('connection_metadata')

    def load_table_metadata(self) -> DataFrame:
        return self.spark.table('table_metadata')