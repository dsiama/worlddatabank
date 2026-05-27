# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "eb1de4c1-a5f7-4384-a481-f546d17e9c96",
# META       "default_lakehouse_name": "e_ext_ingestions_support_lh",
# META       "default_lakehouse_workspace_id": "be4cf150-0cbc-4153-abd4-999a080c1e9d",
# META       "known_lakehouses": [
# META         {
# META           "id": "eb1de4c1-a5f7-4384-a481-f546d17e9c96"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************


main_set = {
                'lh_bronze': 'abfss://33f78fb2-ff90-471a-a7e1-81459e9db472@onelake.dfs.fabric.microsoft.com/0a29beb1-b606-4806-a072-8d15c66feb87',
                'lh_silver': 'abfss://33f78fb2-ff90-471a-a7e1-81459e9db472@onelake.dfs.fabric.microsoft.com/0a29beb1-b606-4806-a072-8d15c66feb87',
                'lh_gold': 'abfss://16a8b624-fa6d-4dc3-8cf5-8323304a7606@onelake.dfs.fabric.microsoft.com/fdfb29b1-4519-4b74-af24-dc52ca15fb61',
                'folder_destino': 'Files/world_data/gapminder',
                'pipeline_info': {
                        'workspace': 'e_ext_ingestions_ws',
                        'pipeline_id': 'e_ext_worldData_gapMinder_ws',
                        'log_level': 5},
                'group_context': {
                        'group_name': 'gapMinder-all', 'run_withouth_new_data': True},
                'task_config': {
                        'order': 2,
                        'nb': 'gapminder_SILVER',
                        'type': 'notebook',
                        'movement_type': 'bronze_to_silver',
                        'enabled': True,
                        'indicators' : []
                }                        
            }



running_local = True
log_correlation = {}



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import io
import requests
import pandas as pd
from pyspark.sql import functions as F
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import time
import json

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if type(main_set) == type("") :
    main_set=json.loads(main_set)
    log_correlation = json.loads(log_correlation)

import sys 
sys.path.append("/lakehouse/default/Files/Global_Libs")
from dap_ELogger import ELogger
from dap_fabric_semantic_tools import semantic_model_refresh_wait
from dap_fabric_config_tools import read_config_pipelines,handleError,build_call_nb_params,update_fabric_metadata,run_notebook_with_params,get_execution_plan


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    nb = "gapminder_SILVER"
    group = ""

    errorState = None
    errorId = 0

    #config_file = main_set["config_file"]
    #local_updates = main_set["local_updates"]

except Exception as e:
    handleError(10 , e, running_local, {"step": "pipeline config"})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


if type(main_set) == type("") :
    main_set=json.loads(main_set)
    log_correlation = json.loads(log_correlation)


errorState = None
errorId = 0


try: 

    lh_bronze = main_set['lh_bronze']
    lh_silver = main_set['lh_silver']

    folder_destino = main_set['folder_destino']

    logLevel = main_set['pipeline_info']["log_level"]
    printOnScreen = main_set['pipeline_info']["log_level"]

    pipeline_id = main_set['pipeline_info']["pipeline_id"]
    ws = main_set['pipeline_info']["workspace"]
    group = main_set["group_context"]["group_name"]
    nb = main_set['task_config']["nb"]



except Exception as e:
    handleError(20 , e, running_local, {"status": "start"})


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


try:
    # Caminhos ABFSS base (Ajuste as variáveis conforme o seu ambiente)
    path_bronze_concepts = f"{lh_bronze}/Tables/world_data_gapminder/bronze_dim_concepts"
    path_silver_indicators = f"{lh_silver}/Tables/world_data_gapminder/gapminder_silver_dim_indicators"
    path_silver_tags_rel = f"{lh_silver}/Tables/world_data_gapminder/gapminder_silver_rel_indicator_tags"

    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] A iniciar processamento da camada Silver para Metadados...\n")


    # 1. LEITURA DA BRONZE VIA ABFSS
    # Filtramos apenas os conceitos que são efetivamente métricas/indicadores (measures)
    df_bronze_concepts = spark.read.format("delta").load(path_bronze_concepts) \
        .filter(F.col("concept_type") == "measure")

    # 2. TABELA 1: DIMENSÃO DE INDICADORES (Limpa e Tipificada)
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] A estruturar dimensão de indicadores... ", end="", flush=True)
    
    df_silver_indicators = df_bronze_concepts.select(
        F.col("concept").alias("indicator_id"),
        F.col("name").alias("indicator_name"),
        F.col("description").alias("indicator_description"),
        F.current_timestamp().alias("_ingested_at")
    ).distinct()
    
    # Gravação única via Overwrite no OneLake Silver
    df_silver_indicators.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(path_silver_indicators)
        
    print(f"Sucesso! ({df_silver_indicators.count()} indicadores registados)")

    # 3. TABELA 2: RELAÇÃO N:M (Explode de Tags para linhas individuais)
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] A gerar tabela de associação N:M (Indicators <-> Tags)... ", end="", flush=True)
    
    # Tratamos nulos, limpamos espaços em branco extras e explodimos a lista por vírgula
    df_silver_tags_rel = df_bronze_concepts \
        .select(
            F.col("concept").alias("indicator_id"),
            F.coalesce(F.col("tags"), F.lit("sem_tag")).alias("tags_raw")
        ) \
        .withColumn("tag_array", F.split(F.col("tags_raw"), ",")) \
        .withColumn("tag_clean", F.explode(F.col("tag_array"))) \
        .withColumn("tag_clean", F.trim(F.col("tag_clean"))) \
        .filter(F.col("tag_clean") != "") \
        .select(
            F.col("indicator_id"),
            F.col("tag_clean").alias("tag_name"),
            F.current_timestamp().alias("_ingested_at")
        ).distinct()

    # Gravação única via Overwrite no OneLake Silver
    df_silver_tags_rel.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(path_silver_tags_rel)
        
    print(f"Sucesso! ({df_silver_tags_rel.count()} relações N:M geradas)")

except Exception as e:
    handleError(30 , e, running_local, {"step": "pipeline config"})

print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Concluída a estruturação de metadados na Silver.")

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

try:
    # Carrega a tabela de relação a partir do caminho ABFSS da Silver
    df_tags = spark.read.format("delta").load(path_silver_tags_rel)

    # Executa o SELECT DISTINCT ordenado alfabeticamente
    # df_tags.select("tag_name").distinct().orderBy("tag_name").show(n=100, truncate=False)

    total_tags = spark.read.format("delta").load(path_silver_tags_rel).select("tag_name").distinct().count()
    total_tags
except Exception as e:
    handleError(40 , e, running_local, {"step": "pipeline config"})

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

path_bronze_geo = f"{lh_bronze}/Tables/world_data_gapminder/bronze_dim_geo"
path_silver_geo = f"{lh_silver}/Tables/world_data_gapminder/gapminder_silver_dim_geo"

def process_gapminder_silver_dim_geo():
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] A processar dim_geo para a Silver...")
    
    # Leitura da Bronze
    df_bronze_geo = spark.read.format("delta").load(path_bronze_geo)
    
    # Seleção, renomeação e tipificação de coordenadas
    df_silver_geo = df_bronze_geo.select(
        F.col("country").alias("geo_id"),
        F.col("name").alias("country_name"),
        F.col("world_4region").alias("macro_region"),
        F.col("world_6region").alias("detail_region"),
        F.col("latitude").cast("double"),
        F.col("longitude").cast("double"),
        F.col("un_state").alias("is_un_state")
    ).distinct()
    
    # Escrita Silver via ABFSS
    df_silver_geo.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(path_silver_geo)
        
    print(f"Sucesso! Tabela dim_geo gravada com {df_silver_geo.count()} países.\n")

try:
    # Executar
    process_gapminder_silver_dim_geo()
except Exception as e:
    handleError(50 , e, running_local, {"step": "pipeline config"})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

path_bronze_fact = f"{lh_bronze}/Tables/world_data_gapminder/{'gapminder_bronze_fact'}"
path_silver_fact = f"{lh_silver}/Tables/world_data_gapminder/gapminder_silver_fact"

def process_gapminder_silver_fact():
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] A processar tabela de FACTOS para a Silver (com Opção A)...")
    
    # 1. Leitura dos dados brutos da Bronze
    df_bronze = spark.read.format("delta").load(path_bronze_fact)
    
    # 2. Padronização da coluna 'value' para limpar espaços em branco
    df_with_clean_string = df_bronze.withColumn("value_trim", F.trim(F.col("value")))
    
    # 3. Transformações, Limpeza de Notação Científica e Tipificação
    df_silver_fact = df_with_clean_string \
        .withColumn("year", F.col("time").cast("integer")) \
        .withColumn("numeric_value", 
            F.when(F.col("value_trim").endswith("k"), 
                   F.expr("substring(value_trim, 1, length(value_trim)-1)").cast("double") * 1000)
             .when(F.col("value_trim").endswith("M"), 
                   F.expr("substring(value_trim, 1, length(value_trim)-1)").cast("double") * 1000000)
             .when(F.col("value_trim").endswith("B"), 
                   F.expr("substring(value_trim, 1, length(value_trim)-1)").cast("double") * 1000000000)
             .otherwise(F.col("value_trim").cast("double")) # Se for um número normal, faz cast direto
        ) \
        .filter(
            # Remove linhas que continuem nulas após a conversão
            F.col("numeric_value").isNotNull() & 
            # Filtro de sanidade temporal
            (F.col("year") >= 1800) & (F.col("year") <= 2100)
        ) \
        .select(
            F.col("geo").alias("geo_id"),
            F.col("year"),
            F.col("_source_indicator").alias("indicator_id"),
            F.col("numeric_value").alias("value"),
            F.current_timestamp().alias("_processed_at")
        )
    
    # 4. Ativação do V-Order (Otimização extra do Fabric para o Power BI)
    spark.conf.set("spark.sql.parquet.vorder.enabled", "true")
    
    # 5. Gravação Otimizada no OneLake via ABFSS
    df_silver_fact.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .partitionBy("indicator_id") \
        .save(path_silver_fact)
        
    print(f"Sucesso! Factos convertidos (K/M/B), limpos e gravados na Silver com V-Order.")

try:
    process_gapminder_silver_fact()
except Exception as e:
    handleError(60 , e, running_local, {"step": "pipeline config"})

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

try:
    path_b_4region = f"{lh_bronze}/Tables/world_data_gapminder/bronze_dim_geo_world_4region"
    path_b_sdg     = f"{lh_bronze}/Tables/world_data_gapminder/bronze_dim_geo_un_sdg_region"

    path_s_4region = f"{lh_silver}/Tables/world_data_gapminder/gapminder_silver_dim_geo_world_4region"
    path_s_sdg     = f"{lh_silver}/Tables/world_data_gapminder/gapminder_silver_dim_geo_un_sdg_region"

    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] A refinar sub-dimensões geográficas na Silver...")

    # Ativação do V-Order na Silver
    spark.conf.set("spark.sql.parquet.vorder.enabled", "true")

    # Processar 4 Regiões
    df_b_4region = spark.read.format("delta").load(path_b_4region)
    df_s_4region = df_b_4region.select(
        F.col("world_4region").alias("macro_region_id"),
        F.trim(F.col("name")).alias("macro_region_name")
    ).distinct()
    df_s_4region.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path_s_4region)

    # Processar Regiões Nações Unidas (SDG)
    df_b_sdg = spark.read.format("delta").load(path_b_sdg)
    df_s_sdg = df_b_sdg.select(
        F.col("un_sdg_region").alias("sdg_region_id"),
        F.trim(F.col("name")).alias("sdg_region_name")
    ).distinct()
    df_s_sdg.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path_s_sdg)

    print("Sub-dimensões geográficas prontas na Silver!")
except Exception as e:
    handleError(70 , e, running_local, {"step": "pipeline config"})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

status = "OK"
result = {}

result = {"new_data": 1}

display(result)
print(datetime.now())

if not running_local:    
    notebookutils.notebook.exit(json.dumps(result))

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
