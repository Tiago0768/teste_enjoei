from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
from pyspark.sql.types import StructType, StructField, StringType, DateType
import requests
import json
from datetime import datetime

# Inicializa a sessão do Spark
spark = SparkSession.builder \
    .appName("API_ENJOEI") \
    .getOrCreate()

# Define o esquema dos dados
schema = StructType([
    StructField("id", StringType(), True),
    StructField("userId", StringType(), True),
    StructField("date", DateType(), True),
    StructField("products", StringType(), True)
])

# URL da API
start_date = "2019-12-10"
end_date = "2020-10-10"
url = f"https://fakestoreapi.com/carts?startdate={start_date}&enddate={end_date}"

# Obtém os dados da API
response = requests.get(url)
if response.status_code == 200:
    json_data = response.json()

    df = spark.createDataFrame(json_data, schema=schema)

    # Converte a coluna 'date' para o formato de data
    df = df.withColumn("date", col("date").cast("date"))

    # Define a tabela no formato do Hive
    base_api = "teste_teste"
    df.createOrReplaceTempView(base_api)

    # Verificação de existencia da tabela
    if spark.catalog._jcatalog.tableExists(base_api):
        result = spark.sql(f"SELECT COUNT(*) FROM {base_api}").collect()[0][0]
    else:
        result = 0

    df = df.withColumn("products", col("products").cast("string"))

    # inserção de dados ou atualização caso a base exista
    if result > 0:
        df.write.mode("overwrite").format("hive").saveAsTable(base_api)
    else:
        df.write.mode("append").format("hive").saveAsTable(base_api)

    print("Dados inseridos/atualizados com sucesso!")
else:
    print(f"Erro ao acessar a API: Status code {response.status_code}")
    
spark.stop()
