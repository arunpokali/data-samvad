from langchain.chat_models import ChatOllama
from pyspark_ai import SparkAI
from langchain.callbacks.manager import CallbackManager
from langchain.callbacks.streaming_stdout import StreamingStdOutCallbackHandler

from pyspark.sql import SparkSession

llm = ChatOllama(model="mistral:latest")


spark_ai = SparkAI(llm=llm, vector_store_dir="vector_store/")
spark_ai.activate()

data = [('Toyota', 1849751, -9), ('Ford', 1767439, -2), ('Chevrolet', 1502389, 6),
        ('Honda', 881201, -33), ('Hyundai', 724265, -2), ('Kia', 693549, -1),
        ('Jeep', 684612, -12), ('Nissan', 682731, -25), ('Subaru', 556581, -5),
        ('Ram Trucks', 545194, -16), ('GMC', 517649, 7), ('Mercedes-Benz', 350949, 7),
        ('BMW', 332388, -1), ('Volkswagen', 301069, -20), ('Mazda', 294908, -11),
        ('Lexus', 258704, -15), ('Dodge', 190793, -12), ('Audi', 186875, -5),
        ('Cadillac', 134726, 14), ('Chrysler', 112713, -2), ('Buick', 103519, -42),
        ('Acura', 102306, -35), ('Volvo', 102038, -16), ('Mitsubishi', 102037, -16),
        ('Lincoln', 83486, -4), ('Porsche', 70065, 0), ('Genesis', 56410, 14),
        ('INFINITI', 46619, -20), ('MINI', 29504, -1), ('Alfa Romeo', 12845, -30),
        ('Maserati', 6413, -10), ('Bentley', 3975, 0), ('Lamborghini', 3134, 3),
        ('Fiat', 915, -61), ('McLaren', 840, -35), ('Rolls-Royce', 460, 7)]

def create_spark_ai_df(data):
    df = spark_ai._spark.createDataFrame(data, ["Brand", "US_Sales_2022", "Sales_Change_Percentage"])

auto_df = spark_ai._spark.createDataFrame(data, ["Brand", "US_Sales_2022", "Sales_Change_Percentage"])

plot_prompt ="Use this dataframe to plot the chart."

auto_df.ai.plot(plot_prompt)
# csv_df = spark_ai._spark.read.csv('titanic.csv')

while True:

    query = input("\nEnter a query: ")
    if query == "exit":
        break
    if query.strip() == "":
        continue

    result = auto_df.ai.transform(query)
    result.show()
    

#     print(type(result))

    while True:

        plot_input = input("\n Want to plot this data ? (Yes/No):")

        if plot_input.strip() == "Yes" or plot_input.strip() == "yes":
            plot_query = input("\n Provide plot prompt: ")
            
            result.ai.plot(plot_prompt + plot_query)
        #     spark_ai.plot_df(result, plot_prompt + plot_query)
        else:
            break
    
    