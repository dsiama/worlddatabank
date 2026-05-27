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
                        'order': 1,
                        'nb': 'gapminder_BRONZE',
                        'datasource': {
                            'id': 'gapMinder_rest_endpoint',
                            'type': 'external_tool_endpoint',
                            'gapMinderMaster': 'https://raw.githubusercontent.com/open-numbers/ddf--gapminder--systema_globalis/master'
                        },
                        'type': 'notebook',
                        'movement_type': 'external_to_bronze_Delta',
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
    nb = "gapminder_BRONZE"
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
    gapMinderMaster = main_set['task_config']['datasource']['gapMinderMaster']
    folder_destino = main_set['folder_destino']

    logLevel = main_set['pipeline_info']["log_level"]
    printOnScreen = main_set['pipeline_info']["log_level"]

    pipeline_id = main_set['pipeline_info']["pipeline_id"]
    ws = main_set['pipeline_info']["workspace"]
    group = main_set["group_context"]["group_name"]
    nb = main_set['task_config']["nb"]

    indicators = main_set['task_config']["indicators"]
    if indicators == []:
        indicators = None


except Exception as e:
    handleError(20 , e, running_local, {"status": "start"})


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

elogger = None
if running_local:
    elogger = ELogger(workspace=ws,pipeline_id=pipeline_id,group = group ,nb=nb,logLevel=5,printOnScreen=True)
    elogger.log_event_start(message= f"group:{group}")
else: 
    elogger = ELogger(correlation=log_correlation,workspace=ws,nb=nb)     
    #elogger.log_event( step_name = "error nb", message = "test", level= 1,  extra = "")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==========================================
# 1. FUNÇÃO: CATÁLOGO DE INDICADORES
# ==========================================
def get_gapminder_indicators_dict() -> dict:
    """
    Lê o inventário global de conceitos do Gapminder via REST 
    e retorna um dicionário {id_indicador: nome_indicador}.
    """
    url = f"{gapMinderMaster}/ddf--concepts.csv"
    response = requests.get(url)
    if response.status_code != 200:
        raise RuntimeError(f"Falha ao aceder ao catálogo: {response.status_code}")
        
    pdf = pd.read_csv(io.StringIO(response.text))
    # Filtra apenas métricas numéricas (datapoints)
    pdf_measures = pdf[pdf['concept_type'] == 'measure']
    
    return dict(zip(pdf_measures['concept'], pdf_measures['name']))


# ==========================================
# 2. FUNÇÕES DE SUPORTE À INGESTÃO (BRONZE)
# ==========================================
def extract_indicator(indicator_id: str) -> pd.DataFrame:
    url = f"{gapMinderMaster}/countries-etc-datapoints/ddf--datapoints--{indicator_id}--by--geo--time.csv"
    
    response = requests.get(url)
    if response.status_code == 404:
        return None  # Ignora granularidades inexistentes nesta pasta padrão
    if response.status_code != 200:
        raise RuntimeError(f"Erro HTTP {response.status_code} em {indicator_id}")
        
    return pd.read_csv(io.StringIO(response.text))



def load_batch_to_bronze_delta(pdf_list: list, indicator_ids: list, table_name: str, mode: str = "append") -> None:
    """
    Remove todos os indicadores/dados antigos e grava os novos de uma só vez.
    Garante operação 100% orientada ao storage ABFSS (OneLake).
    """
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F
    import os

    if not pdf_list:
        return

    # Construção do caminho físico absoluto via ABFSS
    abfss_path = f"{lh_bronze}/Tables/world_data_gapminder/{table_name}"

    # 1. Consolida os DataFrames do Pandas num único DataFrame Spark
    spark_df_list = []
    for pdf, ind_id in zip(pdf_list, indicator_ids):
        df_sp = spark.createDataFrame(pdf).withColumn("_source_indicator", F.lit(ind_id))
        spark_df_list.append(df_sp)
        
    df_spark_bulk = spark_df_list[0]
    for df_sp in spark_df_list[1:]:
        df_spark_bulk = df_spark_bulk.unionByName(df_sp, allowMissingColumns=True)
        
    df_spark_bulk = df_spark_bulk.withColumn("_ingested_at", F.current_timestamp())

    # 2. Validação de existência REAL no ABFSS utilizando a API nativa do Delta Lake
    # Substitui o spark.catalog.tableExists que falha em caminhos puros externos
    table_exists_in_abfss = DeltaTable.isDeltaTable(spark, abfss_path)

    # 3. Aplica o DELETE condicional diretamente sobre o caminho ABFSS
    if mode == "append" and table_exists_in_abfss:
        delta_table = DeltaTable.forPath(spark, abfss_path)
        delta_table.delete(F.col("_source_indicator").isin(indicator_ids))

    # 4. Gravação idempotente orientada ao PATH com registo automático no Fabric
    # Se o modo for overwrite e a tabela já existir, limpamos o caminho físico primeiro para evitar locks
    print(f"mode: {mode}")
    if mode == "overwrite" and table_exists_in_abfss:
        # Força o overwrite do esquema e dos dados diretamente na diretoria ABFSS
        df_spark_bulk.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(abfss_path)
    else:
        # Append standard ou criação inicial do zero
        df_spark_bulk.write \
            .format("delta") \
            .mode(mode) \
            .option("mergeSchema", "true") \
            .option("path", abfss_path) \
            .saveAsTable(table_name)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def process_gapminder_fact_batch(indicators_to_process: list, indicators_dict: dict) -> dict:
    processed_count = 0
    total_records_accumulated = 0
    collected_pdfs = []
    collected_ids = []
    start_all_read = time.time()
    
    print(f"A iniciar processamento de lote com {len(indicators_to_process)} indicadores...\n")
    
    for ind_id in indicators_to_process:
        processed_count+=1
        try:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] a processar indicador #{processed_count:>4}: {ind_id:>20.20} ", end="", flush=True)
            # Extração dos dados da API externa
            start_read = time.time()
            pdf = extract_indicator(ind_id)
            end_read = time.time()
            read_duration = end_read - start_read
            
            if pdf is not None and not pdf.empty:
                current_records = len(pdf)
                #print(pdf.columns)
                pdf.columns = ['geo', 'time', 'value'] # Padroniza colunas
                print(f" records:{current_records:>8} read: {read_duration:>2.1f}s indicador:{indicators_dict.get(ind_id, 'Nome não disponível')} " )
                #print(pdf.columns)
                collected_pdfs.append(pdf)
                collected_ids.append(ind_id)
                total_records_accumulated += current_records
                
        except Exception as e:
            print(f"ERRO API -> {str(e)}")
            continue

    total_read_duration = time.time() - start_all_read
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]  leitura {total_read_duration:>2.1f}s  records #{total_records_accumulated}  Extração concluída. A iniciar GRAVAÇÃO UNIFICADA no Delta Lake...")
    if collected_pdfs:
        start_write = time.time()
        load_batch_to_bronze_delta(
            pdf_list=collected_pdfs, 
            indicator_ids=collected_ids, 
            table_name="gapminder_bronze_fact"
        )
        write_duration = time.time() - start_write
        print(f"-> Tempo único de OneLake (Escrita): {write_duration:>5.1f}s")
        print(f"{total_records_accumulated} records")

    return {
        "processed_indicators": processed_count,
        "total_records": total_records_accumulated
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def extract_dimension(dim_name: str) -> pd.DataFrame:
    """
    Extrai as tabelas de dimensão mapeando os nomes lógicos 
    para os ficheiros físicos reais do repositório Gapminder.
    """
    if dim_name == "concepts":
        url = f"{gapMinderMaster}/ddf--concepts.csv"
        
    elif dim_name == "geo":
        # No Gapminder, a entidade 'geo' real é o ficheiro de 'country'
        url = f"{gapMinderMaster}/ddf--entities--geo--country.csv"
        
    elif dim_name == "country_mappings":
        url = f"{gapMinderMaster}/ddf--entities--geo--country.csv"
        
    elif dim_name == "geo--world_4region":
        url = f"{gapMinderMaster}/ddf--entities--geo--world_4region.csv"
        
    elif dim_name == "geo--un_sdg_region":
        url = f"{gapMinderMaster}/ddf--entities--geo--un_sdg_region.csv"
        
    else:
        url = f"{gapMinderMaster}/ddf--entities--{dim_name}.csv"
        
    response = requests.get(url)

    if response.status_code == 404:
        print(f"(Ficheiro não encontrado no Git: {url}) ", end="")
        return None
        
    if response.status_code != 200:
        raise RuntimeError(f"Erro HTTP {response.status_code} ao extrair {dim_name}")
        
    return pd.read_csv(io.StringIO(response.text))


def process_gapminder_metadata_and_dimensions_bulk():
    dimensions_to_process = [
        "concepts",
        "geo",               # Vai criar a tabela 'bronze_dim_geo' usando o ficheiro country.csv
        "geo--world_4region", 
        "geo--un_sdg_region"
    ]
    tot_records = 0
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] A iniciar processamento de dimensões e metadados...\n")
    
    for idx, dim in enumerate(dimensions_to_process):
        try:
            # Normalização do nome da tabela (ex: geo--world_4region vira bronze_dim_geo_world_4region)
            clean_name = dim.replace("--", "_")
            table_name = f"bronze_dim_{clean_name}"
            
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] #{idx:>4} Processar: {dim:<25} ... ", end="", flush=True)
            
            start_read = time.time()
            pdf = extract_dimension(dim)
            read_duration = time.time() - start_read
            
            if pdf is not None and not pdf.empty:
                current_records = len(pdf)
                
                # Reutilização da função Bulk em modo Overwrite
                start_write = time.time()
                load_batch_to_bronze_delta(
                    pdf_list=[pdf],
                    indicator_ids=[dim],
                    table_name=table_name,
                    mode="overwrite"
                )
                write_duration = time.time() - start_write
                tot_records+=current_records
                print(f"Sucesso! ({current_records:>6} recs) | Read: {read_duration:>4.1f}s | Write: {write_duration:>4.1f}s")
            else:
                print("Sem dados.")
                
        except Exception as e:
            print(f"ERRO -> {str(e)}")
            continue
        return tot_records  
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Camada Bronze de Dimensões e Metadados 100% atualizada.")

# Execução única no Notebook do Fabric
# process_gapminder_metadata_and_dimensions_bulk()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    dim_records = process_gapminder_metadata_and_dimensions_bulk()
    elogger.log_rowCount(step_name="dims",rowCount=dim_records)
except Exception as e:
    handleError(40 , e, running_local, {"status": "start"})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    indicators_dict = get_gapminder_indicators_dict()
    indicators_to_process = indicators if indicators else list(indicators_dict.keys())
    print(f"A processar indicadores #{len(indicators_to_process)}")
    #p = extract_indicator(indicators_to_process[0])
    #len(p)
except Exception as e:
    handleError(50 , e, running_local, {"status": "start"})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:

    r1 = process_gapminder_fact_batch(indicators_to_process=indicators_to_process[0:100],indicators_dict=indicators_dict)
    r1
    fact_records = r1['total_records']
except Exception as e:
    handleError(60 , e, running_local, {"status": "start"})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    r2 = process_gapminder_fact_batch(indicators_to_process=indicators_to_process[101:200],indicators_dict=indicators_dict)
    r2
    fact_records += r2['total_records']
except Exception as e:
    handleError(70 , e, running_local, {"status": "start"})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    r3 = process_gapminder_fact_batch(indicators_to_process=indicators_to_process[201:300],indicators_dict=indicators_dict)
    r3
    fact_records += r3['total_records']
except Exception as e:
    handleError(80 , e, running_local, {"status": "start"})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    r4 = process_gapminder_fact_batch(indicators_to_process=indicators_to_process[301:400],indicators_dict=indicators_dict)
    r4
    fact_records += r4['total_records']
except Exception as e:
    handleError(90 , e, running_local, {"status": "start"})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    r5 = process_gapminder_fact_batch(indicators_to_process=indicators_to_process[401:500],indicators_dict=indicators_dict)
    r5
    fact_records += r5['total_records']
except Exception as e:
    handleError(100 , e, running_local, {"status": "start"})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

status = "OK"
result = {}

elogger.log_rowCount(step_name="facts",rowCount=fact_records)
result = {"dim_records": dim_records,"fact_records": fact_records ,"new_data": 1}

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


path_factos = f"{lh_bronze}/Tables/world_data_gapminder/{'gapminder_bronze_fact'}"
df_factos = spark.read.format("delta").load(path_factos)
df_factos.count()
df_factos.limit(100)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_factos.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_factos.limit(100)

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


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
