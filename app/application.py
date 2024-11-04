from pyspark.sql import SparkSession
from app.myfunc import MyFunc
from common import spark_utils

def main() -> None:
    my_spark = spark_utils.get_spark_session(app_name="ldi")
    application = Application(my_spark)

    num_groups = 5
    # Generate test data with overlapping names returns a list of dataframes
    test_data_list = application.generate_test_data(num_groups=num_groups, overlap=True)

    # Step 1: Union all DataFrames in df_list into a single DataFrame
    #unioned_df = reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), test_data)

    mf = MyFunc(my_spark)
    result = mf.use_loops(test_data_list)


    print("result: ")
    print(result.show(truncate=False))
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

        base_names = ['Alice', 'Bob', 'Charlie', 'David',
                      'Eve', 'James', 'John', 'Casemiro',
                      'Martha', 'Emily', 'Esther', 'Jim',
                      'Rashford', 'Maguire'
                      ]

        for i in range(num_groups):
            # Generate a subset of names with potential overlap
            if overlap:
                names = base_names[:i + 2]  # Increasing overlap with each group
            # Create a DataFrame for the group
            group_df = self._spark.createDataFrame([(name, f'Group_{i}') for name in names], ["names", "group_id"])
            print(group_df.show(truncate=False))
            df_list.append(group_df)

        return df_list



if __name__ == '__main__':
    main()