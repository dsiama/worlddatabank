# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "8e4a8df8-b3c6-44b4-ab93-56ea8afe469f",
# META       "default_lakehouse_name": "WorldDataBank_lh_silver",
# META       "default_lakehouse_workspace_id": "e8c731fe-74bf-4606-8e17-47b318ffbf98",
# META       "known_lakehouses": [
# META         {
# META           "id": "8e4a8df8-b3c6-44b4-ab93-56ea8afe469f"
# META         },
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

# MAGIC %%configure -f
# MAGIC {
# MAGIC     "conf": {
# MAGIC         "spark.fabric.notebook.session.timeout": "72000" 
# MAGIC     }
# MAGIC }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

exec_set = {
    "indicators_iteration_size" : 3,

    "url_call_max_retries":5,
    "url_call_base_delay":2,

    "per_page" : 20000,
    "max_workers" : 8,
    
    "type" : "dims",

    "country" : "all",

    "lh_bronze" : "abfss://e8c731fe-74bf-4606-8e17-47b318ffbf98@onelake.dfs.fabric.microsoft.com/80765be9-c908-44d4-83cb-60a78a964ee2",
    "lh_silver" : "abfss://e8c731fe-74bf-4606-8e17-47b318ffbf98@onelake.dfs.fabric.microsoft.com/8e4a8df8-b3c6-44b4-ab93-56ea8afe469f",
        
    "execType" : {
        "yearsPerIteration" : 5,
        #"type":"lastY"
        #,"distance":4
        "type":"all"
        , "from":1950
        , "to" :2026 
         
    },
}

main_set = {
    "type" : "pipeline_description",


    "worspace" : "WorldDataBank_ws_eng",
    "pipeline_id" : "worlddatabank-facts",
    "name" : "WorldDataBank-facts",

    "datasource_id" : "WDI_API",
    "datasource" : "https://api.worldbank.org/v2/",

    "group" : "6",

    "logLevel" : 4,
    "printOnScreen" : True,

    "next" : {
        "every":"m",
        "distance": 1,
        "days" : [22],
        "hour" : "22h20"
    },
    "pipeline" : [{
        "ord" : 1,
        "type": "nb",
        "name" : "WorldDataBank_nb_bronze_facts",
        "timeout":600,
    },
    {   
        "ord" : 2,
        "type": "nb",
        "name" : "WorldDataBank_nb_silver_facts",
        "timeout":600,}
    ],
    "parameters" : exec_set
}

running_local=True
nb = "WorldDataBank_nb_bronze_facts"
ws = "WorldDataBank_ws_eng"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import sys
from datetime import datetime
from pyspark.sql.functions import explode, col,lower,regexp_replace,lit
import time
import json
import hashlib
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse
from typing import List, Dict, Any, Optional

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


exec_set = main_set["parameters"]

lh_bronze = exec_set["lh_bronze"]
lh_silver = exec_set["lh_silver"]

country = exec_set["country"]

indicators_path = f"{lh_bronze}/Tables/facts/wdi"

group = main_set["group"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

y1=0
y2=0

ypi = exec_set["execType"]["yearsPerIteration"]

if(exec_set["execType"]["type"]=="all"):
    y1 = exec_set["execType"]["from"]
    y2 = exec_set["execType"]["to"] + 1

if(exec_set["execType"]["type"]=="lastY"):
    y2 = datetime.now().year
    y1 = y2 - exec_set["execType"]["distance"]


years = list(range(y1, y2))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# identifica os indicadores para processar

ind_df = (
    spark.read.table("WorldDataBank_lh_silver.dims.indicators")
         .where(col("toImport").isin(group))                 # no SQL string
         .select("indicator_id_original")
         .distinct()
)

indicators = ind_df.select("indicator_id_original").rdd.flatMap(lambda x: x).collect()

# elogger.log_event_info(step_name = "10 - end get identify indicators", message = f"get indicators to run. group:{group}  total {len(indicators)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze = spark.read.format("delta").load(indicators_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(years)
print(indicators)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F, types as T
from delta.tables import DeltaTable

CORE_TBL   = "facts.wdi"
EXTRAS_TBL = "facts.wdi_extras"

YEARS_FILTER = years          # ex.: [2018, 2019, 2020]
INDICATORS_FILTER = indicators #  None     # ex.: ["SP.POP.TOTL", "NY.GDP.MKTP.CD"]

if YEARS_FILTER is not None:
    bronze = bronze.where(F.col("request_year").isin(YEARS_FILTER))

if INDICATORS_FILTER is not None:
    bronze = bronze.where(F.col("indicator_code").isin(INDICATORS_FILTER))

data_array_json = F.get_json_object(F.col("response_json"), "$[1]")

# Schema de uma observação
obs_struct = T.StructType([
    T.StructField("country", T.StructType([
        T.StructField("id", T.StringType(), True),
        T.StructField("value", T.StringType(), True),
    ]), True),
    T.StructField("countryiso3code", T.StringType(), True),
    T.StructField("date", T.StringType(), True),      # "YYYY" | "YYYYMmm" | "YYYYQq"
    T.StructField("value", T.StringType(), True),     # virá como string; vamos castar para double
    T.StructField("indicator", T.StructType([
        T.StructField("id", T.StringType(), True),
        T.StructField("value", T.StringType(), True),
    ]), True),
    # Campos opcionais 
    T.StructField("footnote", T.StringType(), True),
    T.StructField("scale", T.StringType(), True),
    T.StructField("countrycode", T.StringType(), True),
])

obs_array = T.ArrayType(obs_struct)

parsed = bronze.withColumn("obs_array", F.from_json(data_array_json, obs_array))

# -----------------------------
# 3) Explodir (1 observação = 1 linha)
# -----------------------------
exploded = parsed.select(
    "indicator_code",
    F.explode_outer("obs_array").alias("obs")
)

# -----------------------------
# 4) Campos comuns, normalização e tipos
# -----------------------------
# Observação: indicator_id pode vir vazio em alguns casos; usamos indicator_code como fallback
common = exploded.select(
    F.col("indicator_code"),
    F.col("obs.countryiso3code").alias("countryiso3code"),
    F.coalesce(F.col("obs.indicator.id"), F.col("indicator_code")).alias("indicator_id"),
    F.col("obs.date").alias("period"),
    F.col("obs.value").cast("double").alias("value"),
    F.col("obs.footnote").alias("footnote"),
    F.col("obs.scale").alias("scale"),
    F.col("obs.countrycode").alias("countrycode"),
    F.current_timestamp().alias("processed_at")
)

# Derivar 'year' de forma robusta (funciona p/ anual, mensal, trimestral)
common = common.withColumn(
    "year",
    F.regexp_extract("period", r"^([0-9]{4})", 1).cast("int")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------
# 5) Separar em Core e Extras
# -----------------------------
core_df = common.select(
    "indicator_code",
    "countryiso3code",
    "period",
    "year",
    "value",
    "processed_at"
)

extras_df = common.select(
    "indicator_code",
    "countryiso3code",
    "period",
    "year",
    "footnote",
    "scale",
    "countrycode",
    "processed_at"
)

# -----------------------------
# 6) Criar 'row_key' (chave surrogate 1:1) nas duas tabelas
#    Power BI não suporta relação multi-coluna; esta key liga Core ↔ Extras
# -----------------------------
def with_row_key(df):
    return df.withColumn(
        "row_key",
        F.sha2(
            F.concat_ws("|",
                F.coalesce(F.col("indicator_code"), F.lit("")),
                F.coalesce(F.col("countryiso3code"), F.lit("")),
                F.coalesce(F.col("period"), F.lit(""))
            ),
            256
        )
    )

core_df   = with_row_key(core_df)
extras_df = with_row_key(extras_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#%%sql

#CREATE SCHEMA IF NOT EXISTS facts

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sql1 = f"""
CREATE TABLE IF NOT EXISTS {CORE_TBL} (
  indicator_code   STRING,
  countryiso3code  STRING,
  period           STRING,
  year             INT,
  value            DOUBLE,
  processed_at     TIMESTAMP,
  row_key          STRING
) USING DELTA
PARTITIONED BY (year)
"""

sql2 = f"""
CREATE TABLE IF NOT EXISTS {EXTRAS_TBL} (
  indicator_code   STRING,
  countryiso3code  STRING,
  period           STRING,
  year             INT,
  footnote         STRING,
  scale            STRING,
  countrycode      STRING,
  processed_at     TIMESTAMP,
  row_key          STRING
) USING DELTA
PARTITIONED BY (year)
"""

sql3 = f"""
CREATE OR REPLACE VIEW facts.v_wdi_full AS
SELECT
  c.indicator_code,
  c.countryiso3code,
  c.period,
  c.year,
  c.value,
  c.processed_at,
  e.footnote,
  e.scale,
  e.countrycode,
  c.row_key
FROM {CORE_TBL} c
LEFT JOIN {EXTRAS_TBL} e
  ON c.row_key = e.row_key
"""


spark.sql(sql1)
spark.sql(sql2)
spark.sql(sql3)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MERGE idempotente (Core)
#    Chave: row_key (1:1 com Extras)

core_target = DeltaTable.forName(spark, CORE_TBL)

(core_target.alias("t")
 .merge(
     core_df.alias("s"),
     "t.row_key = s.row_key"
 )
 .whenMatchedUpdateAll()
 .whenNotMatchedInsertAll()
 .execute()
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MERGE idempotente (Extras)
# -----------------------------
extra_target = DeltaTable.forName(spark, EXTRAS_TBL)

(extra_target.alias("t")
 .merge(
     extras_df.alias("s"),
     "t.row_key = s.row_key"
 )
 .whenMatchedUpdateAll()
 .whenNotMatchedInsertAll()
 .execute()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sql1 = """
select indicator_code,count(distinct period)periods,min(period)min_period, max(period)max_period,max(processed_at)processed_at ,count(*) ct,count(distinct countrycode) countries 
from facts.v_wdi_full 
group by indicator_code
order by processed_at desc,indicator_code asc
"""


sql2 = """
select * from facts.v_wdi_full limit 50
"""

df = spark.sql(sql1)
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sql1 = """
select i.toImport,count(*) ct ,min(f.processed_at)processed_at_min,max(f.processed_at)processed_at_max 
from facts.wdi f left join dims.indicators i on f.indicator_code = i.indicator_id_original
group by i.toImport
order by 1
"""


df = spark.sql(sql1)
display(df)

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
