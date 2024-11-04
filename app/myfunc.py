from functools import reduce
from os import truncate

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as sf

class MyFunc:

    def __init__(self, spark: SparkSession) -> None:
            self._spark = spark

    def use_loops(self, df):
        distinct_names = set()
        final_groups = []
        print(df.show(truncate=False))
        result = df.agg(sf.collect_set("names")).first()
        #names = set(group.agg(sf.collect_set("names")).first())[0]
        names = set(result[0]) if result else set()
        keep_names = names - distinct_names
        distinct_names.update(names)
        return distinct_names

