import os
import glob
import logging
import itertools
import re
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType
from langchain.chat_models import ChatOllama
from pyspark_ai import SparkAI
from typing import Any, Optional, List

from langchain.prompts.prompt import PromptTemplate
from spark_sql_chain import SparkSQLChain
from pyspark_ai.python_executor import PythonExecutor
from prompt_template import SQL_CHAIN_EXAMPLES, SPARK_SQL_SUFFIX, SPARK_SQL_PREFIX, PLOT_PROMPT_TEMPLATE
from langchain.chains import RetrievalQA
from langchain.embeddings import HuggingFaceEmbeddings
from langchain.callbacks.streaming_stdout import StreamingStdOutCallbackHandler
from langchain.vectorstores import Chroma
from langchain.llms import Ollama
from pyspark_ai.code_logger import CodeLogger
from langchain.schema import HumanMessage, BaseMessage

from pyspark_ai.ai_utils import AIUtils

import chromadb

logger = logging.getLogger()

model = os.environ.get("MODEL", "mistral:7b")
# For embeddings model, the example uses a sentence-transformers model
# https://www.sbert.net/docs/pretrained_models.html 
# "The all-mpnet-base-v2 model provides the best quality, while all-MiniLM-L6-v2 is 5 times faster and still offers good quality."
embeddings_model_name = os.environ.get("EMBEDDINGS_MODEL_NAME", "all-mpnet-base-v2")
persist_directory = os.environ.get("PERSIST_DIRECTORY", "db")
target_source_chunks = int(os.environ.get('TARGET_SOURCE_CHUNKS',4))

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
        self.spark_ai = None
        self.llm = None
        self.dataframes = []
        self.views = []
        self.formatted_common_columns = ""
        self.formatted_sample_data = ""
        self.retrievalQA = None
        self.logger = logging.getLogger()


    def init_spark_session(self):
        """
        Initializes the Spark session.
        """
        logging.info("Initializing Spark session")
        self.spark = SparkSession.builder.appName(self.app_name).getOrCreate()
        self.llm = ChatOllama(model="mistral:7b")
        self.spark_ai = SparkAI(llm=self.llm)
        self.embeddings = HuggingFaceEmbeddings(model_name=embeddings_model_name)
        self.db = Chroma(persist_directory=persist_directory, embedding_function=self.embeddings)


        self.retriever = self.db.as_retriever(search_kwargs={"k": target_source_chunks})
        # activate/deactivate the streaming StdOut callback for LLMs
        # callbacks = [] if args.mute_stream else [StreamingStdOutCallbackHandler()]

        self.llm = Ollama(model=model)

        self.retrievalQA = RetrievalQA.from_chain_type(llm=self.llm, chain_type="stuff", retriever=self.retriever, return_source_documents=False)

        self.spark_ai.activate() 


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
            # print(df.collect()[1:5])
            view_data = '\n'.join([", ".join([str(item) for item in row]) for row in df.collect()[1:5]])
            formatted_sample_data.append(f"View Name: {view} , Columns: {df.columns}\n Sample Data : {view_data}")
            # for col in df.columns:
            #     sample_values = df.select(col).limit(3).toPandas()[col].tolist()[1:]
            #     col_type = self.get_col_type(df, col)
            #     sample_data_section = f"({col}, {col_type[1]}, {sample_values}):\n"
            #     formatted_sample_data.append(sample_data_section)

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
    

    @staticmethod
    def extract_code_blocks(text: str) -> List[str]:
        # Pattern to match code blocks wrapped in triple backticks
        code_block_pattern = re.compile(r"```(.*?)```", re.DOTALL)
        triple_backtick_blocks = re.findall(code_block_pattern, text)

        extracted_blocks = []

        # Handle triple backtick blocks
        if triple_backtick_blocks:
            for block in triple_backtick_blocks:
                block = block.strip()
                if block.startswith("python"):
                    block = block.replace("python\n", "", 1)
                elif block.startswith("sql") or block.startswith("SQL"):
                    block = block.replace("sql\n", "", 1)
                elif block.startswith("SQL"):
                    block = block.replace("SQL\n", "", 1)
                extracted_blocks.append(block)

            return extracted_blocks
        else:
            # Check for single backtick block
            if text.startswith("`") and text.endswith("`"):
                return [text.strip("`")]
            return [text]

    
    @staticmethod
    def get_df_schema(df: DataFrame) -> list:
        schema_lst = [f"{name}, {dtype}" for name, dtype in df.dtypes]
        return schema_lst

    def draw_plot(self, df, desc = None, image_name="plot-image.png"):
        # check for necessary plot dependencies
        try:
            import pandas
            import plotly
            import pyarrow
        except ImportError:
            raise Exception(
                "Dependencies for `plot_df` not found. To fix, run `pip install pyspark-ai[plot]`")
        
        instruction = f"The purpose of the plot: {desc}" if desc is not None else ""

        PLOT_PROMPT = PromptTemplate(
            input_variables=["columns", "instruction","image_name"], 
            template=PLOT_PROMPT_TEMPLATE)

        plot_chain = PythonExecutor(
            df=df,
            prompt=PLOT_PROMPT,
            llm=self.spark_ai._llm,
            logger=self.spark_ai._logger,)

        code = plot_chain.run(
            columns=self.get_df_schema(df),
            instruction=instruction,
            image_name=image_name)
        
        print(f"code : {code}")

        df = df.toPandas()
        

        exec(compile(code, "plot_df-CodeGen", "exec"))

    @staticmethod
    def _execute_code(df: DataFrame, code: str):
        import pandas as pd  # noqa: F401

        exec(compile(code, "plot_df-CodeGen", "exec"))

    def _generate_python_with_retries(
        self,
        df: DataFrame,
        messages: str,
        retries: int = 3,
    ) -> str:
        response = self.retrievalQA(messages)
        
        if self.logger is not None:
            self.logger.info(response.content)
        
        # code = AIUtils.extract_code_blocks(response['result'])[0]
        code = "\n".join(self.extract_code_blocks(response['result']))

        try:
            self._execute_code(df, code)
            return code
        except Exception as e:
            if self.logger is not None:
                self.logger.warning("Getting the following error: \n" + str(e))
            if retries <= 0:
                # if we have no more retries, raise the exception
                self.logger.info(
                    "No more retries left, please modify the instruction or modify the generated code"
                )
                return ""
            if self.logger is not None:
                self.logger.info("Retrying with " + str(retries) + " retries left")

            messages = messages + str(response['result'])
            # append the exception as a HumanMessage into messages
            messages = messages + "\n Error Message" + str(e)
            return self._generate_python_with_retries(
                df, messages, retries - 1
            )


    def _generate_code_with_retries(
        self,
        messages: str,
        retries: int = 3,
    ) -> str:

        response = self.retrievalQA(messages)
        code = AIUtils.extract_code_blocks(response['result'])[0]
        try:
            self.spark.sql(code)
            logger.info(f"response from llm : {response['result']}")
            return code
        except Exception as e:
            if self.logger is not None:
                self.logger.warning("Getting the following error: \n" + str(e))
            if retries <= 0:
                # if we have no more retries, raise the exception
                self.logger.info(
                    "No more retries left, please modify the instruction or modify the generated code"
                )
                return ""
            if self.logger is not None:
                self.logger.info("Retrying with " + str(retries) + " retries left")
            
            messages = messages + str(response['result'])
            # append the exception as a HumanMessage into messages
            messages = messages + "\n Error Message" + str(e)
            return self._generate_code_with_retries( messages, retries - 1)


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
            # "common_columns",
            "sample_data_per_view",
            "desc",
        ],
        prefix=SPARK_SQL_PREFIX,
    )

        self.spark_ai._sql_chain = SparkSQLChain(
                    prompt=SQL_CHAIN_PROMPT,
                    llm=self.spark_ai._llm,
                    logger=self.spark_ai._logger,
                    spark=self.spark,
                )
        

        for view, df in zip(self.views, self.dataframes):
            df.createOrReplaceTempView(view)

        prompt = self.spark_ai._sql_chain.run(
            views=self.views,
            # common_columns=self.formatted_common_columns,
            sample_data_per_view=self.formatted_sample_data,
            desc=query,
        )

        answer = self._generate_code_with_retries(prompt)

        # Check if the query result is empty or None
        if answer is None or not answer.strip():
            return ("Failed", "No results found or query execution failed")

        df = self.spark_ai._spark.sql(answer)
        return ("Success", df) 
    


# Example usage:
if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    processor = SparkSQLProcessor()
    processor.init_spark_session()
    processor.process_csv_files()
    processor.format_sql_suffix_from_dfs()

    summary = processor.retrievalQA("Provide summary in less than 150 words of each csv file and how they are related.")

    trends = processor.retrievalQA("Provide top 5 trends observed in less than 30 words each")

    # logger.info(f"summary: {summary['result']}")

    print(f"summary: {summary['result']}")

    print(f"trends: {trends['result']}")

    while True:

        user_query = input("\nEnter a query ( exit ): ")
        if user_query == "exit":
            break
        if user_query.strip() == "":
            continue

        #Retrieve all columns from the Spend_Table and Contract_Table where the PO Numbers match.
        # user_query = "find top 5 suppliers in India with highest spending"  
        # This would come from your Streamlit frontend
        #Find how many contracts each supplier had for different months of year who are based out of India
        query_result, df = processor.execute_query(user_query)
        if query_result == "Success":
            df.show()
        
            print(user_query,query_result)

            plot_query = input("\nGive query to draw plot , ( exit ): ")

            if plot_query == "exit":
                break
            if plot_query.strip() == "":
                continue

            processor.draw_plot(df, desc=plot_query)
