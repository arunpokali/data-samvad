import os
import glob
import logging
import itertools
import re
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from langchain.chat_models import ChatOllama
from pyspark_ai import SparkAI
from langchain.prompts.prompt import PromptTemplate
from pyspark_ai.spark_sql_chain import SparkSQLChain
from prompt_template import SQL_CHAIN_EXAMPLES, SPARK_SQL_SUFFIX, SPARK_SQL_PREFIX

class SparkSQLProcessor:
    """
    A class to process CSV files using Spark SQL and execute queries based on the processed data.
    """

    def __init__(self, source_directory='source_documents', csv_extension='.csv', app_name="Spark SQL Example"):
        """
        Initializes the SparkSQLProcessor with configurable parameters.

        :param source_directory: Directory containing source CSV files.
        :param csv_extension: Extension of the CSV files.
        :param app_name: Name of the Spark application.
        """
        self.source_directory = source_directory
        self.csv_extension = csv_extension
        self.app_name = app_name
        self.spark = None
        self.dataframes = []
        self.views = []
        self.formatted_common_columns = ""
        self.formatted_sample_data = ""

    def init_spark_session(self):
        """
        Initializes the Spark session.
        """
        logging.info("Initializing Spark session")
        self.spark = SparkSession.builder.appName(self.app_name).getOrCreate()

    def process_csv_files(self):
        """
        Processes all CSV files in the specified directory and loads them into Spark DataFrames.
        """
        csv_files = glob.glob(os.path.join(self.source_directory, f"**/*{self.csv_extension}"), recursive=True)
        for csv_file in csv_files:
            logging.info(f"Processing file: {csv_file}")
            schema = self.infer_schema_from_csv_header(csv_file)
            df = self.spark.read.option("header", False).schema(schema).csv(csv_file)
            self.dataframes.append(df)

    def infer_schema_from_csv_header(self, csv_file):
        """
        Infers the schema of a CSV file from its header, removes non-alphanumeric characters,
        and handles duplicate column names by adding a suffix count.

        :param csv_file: The path of the CSV file.
        :return: A StructType schema representing the sanitized CSV header.
        """
        header_df = self.spark.read.option("header", True).csv(csv_file).limit(0)
        original_columns = header_df.columns

        # Remove non-alphanumeric characters and spaces
        sanitized_columns = [re.sub(r'[^a-zA-Z0-9]', '', col) for col in original_columns]

        # Handle duplicate column names
        final_columns = []
        column_count = {}
        for col in sanitized_columns:
            if col not in column_count:
                column_count[col] = 1
                final_columns.append(col)
            else:
                new_col = f"{col}{column_count[col]}"
                column_count[col] += 1
                final_columns.append(new_col)

        # Create a StructType schema with the final column names
        fields = [StructField(col, StringType(), True) for col in final_columns]
        return StructType(fields)

    def format_sql_suffix_from_dfs(self):
        """
        Formats SQL suffixes from the DataFrames and stores formatted common columns and sample data.
        Generates combinations for all possible groups of views.
        """
        self.views = [os.path.splitext(os.path.basename(csv_file))[0] for csv_file in glob.glob(os.path.join(self.source_directory, f"**/*{self.csv_extension}"), recursive=True)]
        
        formatted_combinations = []
        formatted_sample_data = []

        # Generating combinations for all possible group sizes
        for num_views in range(2, len(self.views) + 1):
            view_combinations = itertools.combinations(self.views, num_views)

            for view_combination in view_combinations:
                dfs_for_combination = [df for view, df in zip(self.views, self.dataframes) if view in view_combination]
                common_columns = self.find_common_columns_from_dfs(dfs_for_combination)
                formatted_common_columns = ', '.join(common_columns)
                formatted_combinations.append(f"({' '.join(view_combination)}, [{formatted_common_columns}])")

        for view, df in zip(self.views, self.dataframes):
            formatted_sample_data.append(f"View Name: {view}\n")
            for col in df.columns:
                sample_values = df.select(col).limit(2).toPandas()[col].tolist()
                col_type = self.get_col_type(df, col)
                sample_data_section = f"({col}, {col_type}, {sample_values}):\n"
                formatted_sample_data.append(sample_data_section)

        self.formatted_common_columns = '\n\n'.join(formatted_combinations)
        self.formatted_sample_data = '\n'.join(formatted_sample_data)

    def get_col_type(self, dataframe, col_name):
        """
        Retrieves the data type of a specified column in a DataFrame.

        :param dataframe: The DataFrame to inspect.
        :param col_name: The name of the column whose type is to be determined.
        :return: The data type of the column.
        """
        return [dtype for dtype, col in zip(dataframe.dtypes, dataframe.columns) if col == col_name][0]


    @staticmethod
    def find_common_columns_from_dfs(dataframes):
        """
        Finds common columns among a list of DataFrames.

        :param dataframes: A list of DataFrames to compare.
        :return: A set of common columns.
        """
        column_sets = [set(df.columns) for df in dataframes]
        return set.intersection(*column_sets)

    def execute_query(self, query):
        """
        Executes a given query using the Spark SQL chain and returns the result.

        :param query: The SQL query to be executed.
        :return: The result of the query execution.
        """
        SQL_CHAIN_PROMPT = PromptTemplate.from_examples(
        examples=SQL_CHAIN_EXAMPLES,
        suffix=SPARK_SQL_SUFFIX,
        input_variables=[
            "views",
            "common_columns",
            "sample_data_per_view",
            "desc",
        ],
        prefix=SPARK_SQL_PREFIX,
    )

        llm = ChatOllama(model="codellama:13b")
        spark_ai = SparkAI(llm=llm)
        spark_ai._sql_chain = SparkSQLChain(
                    prompt=SQL_CHAIN_PROMPT,
                    llm=spark_ai._llm,
                    logger=spark_ai._logger,
                    spark=self.spark,
                )
        spark_ai.activate()

        for view, df in zip(self.views, self.dataframes):
            df.createOrReplaceTempView(view)

        sql_query = spark_ai._sql_chain.run(
            views=self.views,
            common_columns=self.formatted_common_columns,
            sample_data_per_view=self.formatted_sample_data,
            desc=query,
        )

        # Check if the query result is empty or None
        if sql_query is None or not sql_query.strip():
            return ("Failed", "No results found or query execution failed")


        df = spark_ai._spark.sql(sql_query)
        return ("Success", df) 

# Example usage:
if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    processor = SparkSQLProcessor()
    processor.init_spark_session()
    processor.process_csv_files()
    processor.format_sql_suffix_from_dfs()

    while True:

        user_query = input("\nEnter a query ( exit ): ")
        if user_query == "exit":
            break
        if user_query.strip() == "":
            continue

        # user_query = "Find top 5 suppliers with highest number of contracts placed"  # This would come from your Streamlit frontend
        query_result, df = processor.execute_query(user_query)
        if query_result == "Success":
            df.show()
        print(user_query,query_result)
