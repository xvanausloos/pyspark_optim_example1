from functools import reduce
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as sf
from pyspark.sql.window import Window


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

    def use_dataframe_properties(self, df: DataFrame, num_groups: int):
        group_ref = ["Group_" + str(i) for i in range(num_groups)]

        mapping_dict = {col_id: idx for idx, col_id in enumerate(group_ref)}

        df_with_mapping = df.withColumn('actual_number',
                                        sf.create_map([sf.lit(x) for x in sum(mapping_dict.items(), ())])[
                                            sf.col('group_id')])

        window_spec = Window.partitionBy("names").orderBy("actual_number")  # Define an appropriate ordering column
        ranked_df = df_with_mapping.withColumn("rank", sf.row_number().over(window_spec))

        return ranked_df.filter(sf.col("rank") == 1)
