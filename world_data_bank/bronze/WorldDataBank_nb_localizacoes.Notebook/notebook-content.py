# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "80765be9-c908-44d4-83cb-60a78a964ee2",
# META       "default_lakehouse_name": "WorldDataBank_lh_bronze",
# META       "default_lakehouse_workspace_id": "e8c731fe-74bf-4606-8e17-47b318ffbf98",
# META       "known_lakehouses": [
# META         {
# META           "id": "80765be9-c908-44d4-83cb-60a78a964ee2"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "default_warehouse": "1c997d3d-987e-4b52-8d0a-76ac185651dc",
# META       "known_warehouses": [
# META         {
# META           "id": "1c997d3d-987e-4b52-8d0a-76ac185651dc",
# META           "type": "Lakewarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

"""
mounts = notebookutils.fs.mounts()
default_lakehouse_mount = next(m for m in mounts if m.mountPoint == "/default").source
default_lakehouse_mount
"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mounts = notebookutils.fs.mounts()
default_lakehouse_mount = next(m for m in mounts if m.mountPoint == "/default").source
default_lakehouse_mount

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import sys
import requests
from pyspark.sql.functions import col, current_timestamp


sys.path.append("/lakehouse/default/Files/Global_Libs")
from fabric_logger import FabricLogger

logger = FabricLogger(app_name="WorldDataBank_nb_localizacoes")
logger.log_event(status="START", message="Catalogo de Paises")

try:
    base_url = "https://api.worldbank.org/v2/country"
    per_page = 150
    all_records = []

    # 2. Primeira chamada para descobrir o total de páginas
    response = requests.get(f"{base_url}?format=json&per_page={per_page}&page=1")
    initial_data = response.json()
    total_pages = initial_data[0]['pages']
    
    # 3. Loop de Paginação
    for page in range(1, total_pages + 1):
        page_response = requests.get(f"{base_url}?format=json&per_page={per_page}&page={page}")
        page_data = page_response.json()
        all_records.extend(page_data[1]) # Adiciona os dados da página à lista total

    # 4. Transformação com PySpark
    df_bruto = spark.createDataFrame(all_records)
    
    df_dim = df_bruto.select(
        col("id").alias("country_code"),
        col("name").alias("country_name"),
        col("region.value").alias("region_name"),
        col("incomeLevel.value").alias("income_level_name"),
        current_timestamp().alias("ingestion_date")
    )

    # 5. Criação da Tabela na Zona Bronze
    target_table = "dim_locations"
    df_dim.write.format("delta").mode("overwrite").saveAsTable(target_table)

    logger.log_event(status="SUCCESS", message=f"Tabela {target_table} criada com {df_dim.count()} registos.")

except Exception as e:
    logger.log_event(status="ERROR", message="Falha na criação da dimensão.", error_details=str(e))
    raise e

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC select * from dbo.dim_locations

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import sys, time, requests
from pyspark.sql.functions import col, lit, current_timestamp

# 1. Obter a lista de códigos da nossa dimensão
# Usamos .collect() para transformar a coluna do Spark numa listimeta Python

def safe_get(url, logger, max_retries=3):
    retries = 0
    backoff_time = 2  # Segundos iniciais de espera

    while retries < max_retries:
        try:
            response = requests.get(url)
            
            # Se for 429 (Too Many Requests), esperamos e tentamos de novo
            if response.status_code == 429:
                logger.log_event(status="WARNING 429", message=f"Limite atingido. Esperando {backoff_time}s...")
                time.sleep(backoff_time)
                retries += 1
                backoff_time *= 2  # Aumenta o tempo de espera (Exponential Backoff)
                continue
            
            # Se houver outro erro (404, 500, etc), levantamos uma exceção
            response.raise_for_status()
            return response.json()

        except Exception as e:
            retries += 1
            if retries == max_retries:
                logger.log_event(status="ERROR", message=f"Falha total após {max_retries} tentativas na URL: {url}", error_details=str(e))
                return None
            time.sleep(backoff_time)


sys.path.append("/lakehouse/default/Files/Global_Libs")
from fabric_logger import FabricLogger
logger = FabricLogger(app_name="WorldDataBank_nb_total_population")


paises = spark.table("dim_locations").select("country_code", "country_name").distinct().collect()

logger.log_event(status="INFO", message=f"Iniciando captura de população para {len(paises)} localizações.")

todas_populacoes = []
cp = len(paises)

try:
    for p in paises:
        code = p.country_code
        nome = p.country_name

        print(cp,nome)
        cp -= 1
        url = f"https://api.worldbank.org/v2/country/{code}/indicator/SP.POP.TOTL?format=json&date=1860:2024&per_page=500"
        
        # response = requests.get(url)
        data = safe_get(url,logger,3)
        #data = response.json()

        # Verificamos se a API devolveu dados válidos (posição [1])
        if len(data) > 1 and data[1] is not None:
            todas_populacoes.extend(data[1])
        else:
            logger.log_event(status="WARNING", message=f"Sem dados de população para o código: {code}")

    # 2. Transformar a lista gigante de resultados num DataFrame Spark
    df_pop_bruto = spark.createDataFrame(todas_populacoes)

    # 3. Limpeza básica para a zona Bronze
    # Extraímos o ID do país e o valor da população, limpando a estrutura aninhada
    df_pop_final = df_pop_bruto.select(
        col("country.id").alias("country_code"),
        col("country.value").alias("country_name"),
        col("date").cast("int").alias("year"),
        col("value").cast("long").alias("total_population"),
        current_timestamp().alias("ingestion_date")
    )

    # 4. Gravação na zona Bronze (Tabela de Factos)
    df_pop_final.write.format("delta").mode("overwrite").saveAsTable("fact_population")

    target_table = "fact_population"

    logger.log_event(status="SUCCESS", message=f"Tabela {target_table} criada com {df_pop_final.count()} registos.")
    

except Exception as e:
    logger.log_event(status="ERROR", message="Erro no loop de população.", error_details=str(e))
    raise e

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM WorldDataBank_lh_bronze.dbo.fact_population  LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select country_name,min(year),count(year)  from WorldDataBank_lh_bronze.dbo.fact_population  group by country_name

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select country_name,year,total_population  from WorldDataBank_lh_bronze.dbo.fact_population where country_name = 'Portugal' order by year 

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


from notebookutils import mssparkutils

# Ver o que existe
#display(mssparkutils.fs.ls("Tables"))

# Apagar a pasta antiga bronze (e tudo o que está lá dentro)

#mssparkutils.fs.rm("Tables/worldbank", True)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Indicadores __________________

# CELL ********************

exec_set = {
    "datasource" : "https://api.worldbank.org/v2/",
    "url_path" : "indicator",

    "url_call_max_retries":5,
    "url_call_base_delay":2,

    "per_page" : 20000,

    "type" : "dims",

    "lh_bronze" : "abfss://e8c731fe-74bf-4606-8e17-47b318ffbf98@onelake.dfs.fabric.microsoft.com/80765be9-c908-44d4-83cb-60a78a964ee2",
    "lh_silver" : "abfss://e8c731fe-74bf-4606-8e17-47b318ffbf98@onelake.dfs.fabric.microsoft.com/8e4a8df8-b3c6-44b4-ab93-56ea8afe469f",
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

datasource = exec_set["datasource"] 
url_path = exec_set["url_path"] 

url_call_max_retries=exec_set["url_call_max_retries"] 
url_call_base_delay=exec_set["url_call_base_delay"] 

per_page = exec_set["per_page"] 

lh_bronze = exec_set["lh_bronze"]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import pandas as pd
from tqdm.auto import tqdm



def request_with_retry(url, params, url_call_max_retries=5, url_call_base_delay=2):
    """
    Suporta exponential backoff: 2s, 4s, 8s, 16s, 32s...
    """
    for attempt in range(url_call_max_retries):
        resp = requests.get(url, params=params)

        # Se não for 429 → devolve logo
        if resp.status_code != 429:
            resp.raise_for_status()
            return resp

        # Caso seja 429 (Too Many Requests)
        wait_time = url_call_base_delay * (2 ** attempt)
        print(f"⚠️ 429 Too Many Requests — a esperar {wait_time} segundos...")
        time.sleep(wait_time)

    # Se chegou ao limite e não resultou:
    raise Exception("Erro 429 persistente após múltiplas tentativas.")


def get_request(url,per_page=20000, url_call_max_retries=5, url_call_base_delay=2):
    params = {"format": "json", "per_page": per_page, "page": 1}
    

    # 1º pedido para descobrir total de páginas
    resp = request_with_retry(url, params)
    data = resp.json()

    metadata = data[0]
    total_pages = metadata.get("pages", 1)

    all_rows = []

    for page in tqdm(range(1, total_pages + 1), desc="A carregar catálogo"):
        params["page"] = page
        r = request_with_retry(url, params)
        d = r.json()
        
        if len(d) > 1 and isinstance(d[1], list):
            all_rows.extend(d[1])

    return pd.json_normalize(all_rows)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

url = f"{datasource}{url_path}"
df_indicators = get_request(url,per_page,url_call_max_retries,url_call_base_delay)
df_indicators["its"] = pd.Timestamp.utcnow()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


spark_df_indicators = spark.createDataFrame(df_indicators)

indicators_path = f"{lh_bronze}/Tables/dims/indicators_catalog"

spark_df_indicators.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(indicators_path)
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import pandas as pd
from tqdm.auto import tqdm
from pyspark.sql.functions import lit, current_timestamp

def get_worldbank_indicator_catalog(per_page=20000):
    """
    Faz download do catálogo de indicadores do World Bank
    com paginação automática e retorna um DataFrame.
    
    Totalmente metadata-driven: lê o número de páginas na primeira chamada
    e itera automaticamente até terminar.
    """
    
    base_url = "https://api.worldbank.org/v2/indicator"
    params = {
        "format": "json",
        "per_page": per_page,
        "page": 1
    }

    # Primeiro pedido → descobre nº total de páginas
    response = requests.get(base_url, params=params)
    response.raise_for_status()

    data = response.json()

    if not isinstance(data, list) or len(data) < 2:
        raise ValueError("Resposta inesperada da API do World Bank.")

    metadata = data[0]
    total_pages = metadata.get("pages", 1)

    print(f"Total de páginas: {total_pages}")

    # Lista para acumular todos os registos
    all_indicators = []

    # Iterar metadados driven
    for page in tqdm(range(1, total_pages + 1), desc="A descarregar catálogo"):
        params["page"] = page
        resp = requests.get(base_url, params=params)
        resp.raise_for_status()

        d = resp.json()
        if len(d) > 1 and isinstance(d[1], list):
            all_indicators.extend(d[1])

    # Normalizar JSON → DataFrame limpo
    df = pd.json_normalize(all_indicators)

    return df


# --- Executar ---
df_indicators = get_worldbank_indicator_catalog()

df_indicators["its"] = pd.Timestamp.utcnow()

print("Número total de indicadores:", len(df_indicators))
df_indicators.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


RAW_INDICATORS_PATH = "Tables/dims/indicators_catalog"

spark_df_indicators = spark.createDataFrame(df_indicators)

(
    spark_df_indicators
        .write
        .format("delta")
        .mode("overwrite")          # ou "append", se quiseres histórico também no Bronze
        .option("overwriteSchema", "true")  # adapta se o schema da API mudar
        .save(BRONZE_INDICATORS_PATH)
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

table_name = "worldbank_indicators_catalog"
table_path = "Tables/dims/indicators_catalog"

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {table_name}
    USING delta
    LOCATION '{table_path}'
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("""
    SELECT DISTINCT `source.value` AS source_value
    FROM worldbank_indicators_catalog
    ORDER BY source_value
""")

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM dims.indicators_catalog;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.table("dims.indicators_catalog")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import explode, col

df_exploded = (
    df
    .select(
        col("id").alias("indicator_id"),
        explode("topics").alias("topic")   # explode array into multiple rows
    )
)

df_topics = (
    df_exploded
    .select(
        "indicator_id",
        col("topic.id").alias("topic_id"),
        col("topic.value").alias("topic_name")
    )
)

df_exploded2 = (
    df_exploded
    .select(
        "indicator_id",
        col("topic.id").alias("topic_id")
    )
)


dim_topic = df_topics.select("topic_id", "topic_name").distinct()
display(df_exploded2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_topic.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("WorldDataBank_lh_raw/Tables/silver/worldbank/dims/dim_topic")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
