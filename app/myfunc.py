from functools import reduce
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as sf


class MyFunc:
    def __init__(self, spark: SparkSession) -> None:
        self._spark = spark

    def use_loops(self, list_df: list):
        distinct_names = set()
        final_groups = []
        for ind, group in enumerate(list_df):
            names = set(group.agg(sf.collect_set("names")).first()[0])
            keep_names = names - distinct_names
            distinct_names.update(names)

            if ind == 0:
                final_groups.append(group)
            else:
                if keep_names:
                    final_groups.append(
                        group.join(
                            sf.broadcast(
                                self._spark.createDataFrame(
                                    [(names,) for names in keep_names],
                                    ["names"],
                                )
                            ),
                            on="names",
                        )
                    )

        return reduce(DataFrame.unionByName, final_groups)
