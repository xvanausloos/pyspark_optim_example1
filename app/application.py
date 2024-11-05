import functools
from datetime import time
from pyspark.sql import SparkSession
from app.myfunc import MyFunc
from common import spark_utils



def main() -> None:
    my_spark = spark_utils.get_spark_session(app_name="ldi")
    application = Application(my_spark)

    # Generate test data with overlapping names returns a list of dataframes
    # test_data_list = application.generate_test_data(num_groups=num_groups, overlap=True)

    # mf = MyFunc(my_spark)
    # result = mf.use_loops(test_data_list)

    # Step 1: Union all DataFrames in df_list into a single DataFrame
    # unioned_df = functools.reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), test_data_list)

    # result2 = mf.use_dataframe_properties(df=unioned_df, num_groups=5)

    use_loops_times, use_dataframe_properties_times = application.compare_two_func(
        num_groups=[1, 3]
    )

    print(f"use_loops_times: {use_loops_times}")
    print(f"use_dataframe_properties_times: {use_dataframe_properties_times}")
    print("*** End ***")


class Application:
    def __init__(self, spark: SparkSession) -> None:
        self._spark = spark

    def generate_test_data(self, num_groups=3, overlap=True) -> list:
        """
        Generate a list of DataFrames with potential overlapping 'names'.

        Parameters:
            spark (SparkSession): The Spark session object.
            num_groups (int): Number of cohorts (DataFrames) to generate.
            overlap (bool): If True, allow overlap of names between group.

        Returns:
            List[DataFrame]: A list of PySpark DataFrames with 'names' overlap.
        """
        df_list = []

        base_names = [
            "Alice",
            "Bob",
            "Charlie",
            "David",
            "Eve",
            "James",
            "John",
            "Casemiro",
            "Martha",
            "Emily",
            "Esther",
            "Jim",
            "Rashford",
            "Maguire",
        ]

        for i in range(num_groups):
            # Generate a subset of names with potential overlap
            if overlap:
                names = base_names[: i + 2]  # Increasing overlap with each group
            # Create a DataFrame for the group
            group_df = self._spark.createDataFrame(
                [(name, f"Group_{i}") for name in names], ["names", "group_id"]
            )
            print(group_df.show(truncate=False))
            df_list.append(group_df)
        return df_list

    def compare_two_func(self, num_groups=[1, 10]) -> (list, list):
        """

        :type num_groups: object
        """
        # Generate test data with overlapping names
        myf = MyFunc
        use_loops_times = []
        use_dataframe_properties_times = []
        for num in num_groups:
            print(f"num group : {num}")
            # person_list = ["Person_" + str(i) for i in range(100)]
            test_data = self.generate_test_data(num_groups=num, overlap=True)
            # Step 1: Union all DataFrames in df_list into a single DataFrame
            unioned_df = functools.reduce(
                lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True),
                test_data,
            )

            # use loops
            start_time = time.time()
            myf.use_loops(spark=self._spark, list_df=test_data).count()
            use_loops_times.append(time.time() - start_time)

            # use dataframe properties
            start_time2 = time.time()
            myf.use_dataframe_properties(df=unioned_df, num_groups=num).count()
            use_dataframe_properties_times.append(time.time() - start_time2)

        return use_loops_times, use_dataframe_properties_times


if __name__ == "__main__":
    main()
