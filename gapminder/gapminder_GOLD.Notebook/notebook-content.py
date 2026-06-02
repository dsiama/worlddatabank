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
                'lh_gold': 'abfss://e8c731fe-74bf-4606-8e17-47b318ffbf98@onelake.dfs.fabric.microsoft.com/3175b382-e687-43bb-aba9-3e6461383d4d',

                'pipeline_info': {
                        'workspace': 'e_ext_ingestions_ws',
                        'pipeline_id': 'e_ext_worldData_gapMinder_ws',
                        'log_level': 5},
                'group_context': {
                        'group_name': 'gapMinder-all', 'run_withouth_new_data': True},
                'task_config': {
                        'order': 3,
                        'nb': 'gapminder_GOLD',
                        'type': 'notebook',
                        'movement_type': 'silver_to_gold',
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
import time
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from concurrent.futures import ThreadPoolExecutor
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

    lh_gold = main_set['lh_gold']
    lh_silver = main_set['lh_silver']

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




try:

    # ==============================================================================
    # 1. CONFIGURAÇÃO DOS CAMINHOS FÍSICOS ABFSS (FONTES 100% SILVER)
    # ==============================================================================
    path_silver_fact       = f"{lh_silver}/Tables/world_data_gapminder/gapminder_silver_fact"
    path_silver_geo        = f"{lh_silver}/Tables/world_data_gapminder/gapminder_silver_dim_geo"
    path_silver_indicators = f"{lh_silver}/Tables/world_data_gapminder/gapminder_silver_dim_indicators"
    path_silver_tags_rel   = f"{lh_silver}/Tables/world_data_gapminder/gapminder_silver_rel_indicator_tags"
    path_silver_4region    = f"{lh_silver}/Tables/world_data_gapminder/gapminder_silver_dim_geo_world_4region"
    path_silver_sdg        = f"{lh_silver}/Tables/world_data_gapminder/gapminder_silver_dim_geo_un_sdg_region"

    # Destinos na Gold
    path_gold_fact         = f"{lh_gold}/Tables/world_data_gapminder/gapminder_gold_analytics_fact"
    path_gold_dim_geo      = f"{lh_gold}/Tables/world_data_gapminder/gapminder_gold_dim_geo"
    path_gold_dim_ind      = f"{lh_gold}/Tables/world_data_gapminder/gapminder_gold_dim_indicators"
    path_gold_rel_tags     = f"{lh_gold}/Tables/world_data_gapminder/gapminder_gold_rel_indicator_tags"

    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] A INICIAR ORQUESTRAÇÃO DA CAMADA GOLD (PURIFICADA)...\n")
    start_pipeline = time.time()

    spark.conf.set("spark.sql.parquet.vorder.enabled", "true")

    # ==============================================================================
    # 2. LEITURA DOS DADOS EXCLUSIVAMENTE DA SILVER
    # ==============================================================================
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] A carregar tabelas da Silver... ", end="", flush=True)
    df_sil_fact   = spark.read.format("delta").load(path_silver_fact)
    df_sil_geo    = spark.read.format("delta").load(path_silver_geo)
    df_sil_ind    = spark.read.format("delta").load(path_silver_indicators)
    df_sil_tags   = spark.read.format("delta").load(path_silver_tags_rel)
    df_sil_4reg   = spark.read.format("delta").load(path_silver_4region)
    df_sil_sdg    = spark.read.format("delta").load(path_silver_sdg)
    print("Sucesso!")

    # ==============================================================================
    # 3. CONSOLIDAÇÃO DA DIMENSÃO GEOGRÁFICA (Cruzando dados Silver com Silver)
    # ==============================================================================
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] A processar dim_geo Gold... ", end="", flush=True)
    df_gold_geo = df_sil_geo \
        .join(df_sil_4reg, df_sil_geo["macro_region"] == df_sil_4reg["macro_region_id"], "left") \
        .join(df_sil_sdg, df_sil_geo["detail_region"] == df_sil_sdg["sdg_region_id"], "left") \
        .select(
            df_sil_geo["geo_id"],
            df_sil_geo["country_name"],
            df_sil_4reg["macro_region_id"],
            df_sil_4reg["macro_region_name"],
            df_sil_sdg["sdg_region_id"],
            df_sil_sdg["sdg_region_name"],
            df_sil_geo["latitude"],
            df_sil_geo["longitude"],
            df_sil_geo["is_un_state"],
            df_sil_geo["main_religion_2008"]
        ).distinct()

    df_gold_geo.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path_gold_dim_geo)
    print(f"Sucesso! ({df_gold_geo.count()} países)")

    # ==============================================================================
    # 4. PASSTHROUGH DE METADADOS E 5. FACTOS DEDUPLICADOS
    # ==============================================================================
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] A migrar dim_indicators e rel_tags para a Gold... ", end="", flush=True)
    df_sil_ind.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path_gold_dim_ind)
    df_sil_tags.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path_gold_rel_tags)
    print("Sucesso!")

    df_gold_fact_joined = df_sil_fact.join(
            df_gold_geo, df_sil_fact["geo_id"] == df_gold_geo["geo_id"], "inner"
        ).select(
            df_sil_fact["geo_id"],
            df_gold_geo["macro_region_name"].alias("continent"), 
            df_sil_fact["year"],
            df_sil_fact["indicator_id"],
            df_sil_fact["value"].cast("double")
        )

    # 2. ESCUDO DE PROTEÇÃO: Garantir que não há duplicados antes do ranking
    df_dedup = df_gold_fact_joined.dropDuplicates(["geo_id", "year", "indicator_id"])

    # ==============================================================================
    # LÓGICA DO ÍNDICE: Criar uma coluna auxiliar que ignora os anos futuros (> 2026)
    # ==============================================================================
    df_with_conditional_year = df_dedup.withColumn(
        "_year_for_ranking",
        F.when(F.col("year") <= 2026, F.col("year")).otherwise(F.lit(None))
    )

    # O 'desc_nulls_last()' garante que os anos futuros  fiquem no fim da fila
    window_spec = Window.partitionBy("geo_id", "indicator_id").orderBy(F.col("_year_for_ranking").desc_nulls_last())

    # 3. Aplicar Ranking e forçar o valor 0 para o futuro
    df_gold_fact_final = df_with_conditional_year \
        .withColumn("year_rank_index", 
            F.when(F.col("year") > 2026, F.lit(0)) # Se for futuro, o índice é sempre 0
             .otherwise(F.dense_rank().over(window_spec)) # Se for passado/presente, conta 1, 2, 3...
        ) \
        .drop("_year_for_ranking") \
        .withColumn("_gold_updated_at", F.current_timestamp())

    # ==============================================================================
    # 4. GRAVAÇÃO E ATUALIZAÇÃO NO LAKEHOUSE
    # ==============================================================================
    spark.conf.set("spark.sql.parquet.vorder.enabled", "true")
    
    df_gold_fact_final.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .partitionBy("continent", "year") \
        .save(path_gold_fact)
        
    print(f"Sucesso! Tabela reconstruída com a coluna 'year_rank_index'.")


except Exception as e:
    handleError(30 , e, running_local, {"step": ""})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

path_gold_dim_date = f"{lh_gold}/Tables/world_data_gapminder/gapminder_gold_dim_date"

print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] A gerar dimensão de tempo (dim_date) para a Gold...")

try:
    # 1. sequência de anos (ex: de 1800 até 2100)
    df_years = spark.range(1800, 2101).withColumnRenamed("id", "year")
    
    # 2. Construímos os atributos de data padrão para o Power BI
    df_dim_date = df_years.withColumn(
        "date_id", 
        F.to_date(F.concat(F.col("year"), F.lit("-01-01")), "yyyy-MM-dd")
    ).select(
        F.col("year").cast("integer"),
        F.col("date_id"),                                           # Chave do tipo Date para o Power BI
        F.lit("Jan 1").alias("period_name"),
        F.concat(F.lit("CY "), F.col("year")).alias("year_label")   # Ex: "CY 2026"
    )
    
    # 3. Gravação na Gold com V-Order
    spark.conf.set("spark.sql.parquet.vorder.enabled", "true")
    df_dim_date.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path_gold_dim_date)
    
    # 4. Catálogo
    if not spark.catalog.tableExists("gapminder_gold_dim_date"):
        spark.catalog.createTable("gapminder_gold_dim_date", path=path_gold_dim_date, source="delta")
        
    print(f"Sucesso! dim_date gerada com {df_dim_date.count()} anos mapeados.")

except Exception as e:
    handleError(40 , e, running_local, {"step": ""})

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
