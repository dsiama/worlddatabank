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
# META         },
# META         {
# META           "id": "8e4a8df8-b3c6-44b4-ab93-56ea8afe469f"
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
        #,"distance":5
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

    "group" : "9",

    "logLevel" : 5,
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

datasource = main_set["datasource"] 


url_call_max_retries=exec_set["url_call_max_retries"] 
url_call_base_delay=exec_set["url_call_base_delay"] 

per_page = exec_set["per_page"] 
max_workers =int(exec_set["max_workers"])

lh_bronze = exec_set["lh_bronze"]
lh_silver = exec_set["lh_silver"]

country = exec_set["country"]

url = f"{datasource}country/{country}/indicator/"
indicators_path = f"{lh_bronze}/Tables/facts/wdi"

indicators_iteration_size = exec_set["indicators_iteration_size"]

logLevel=main_set["logLevel"] 

group = main_set["group"]


pipeline_id = main_set["pipeline_id"]
printOnScreen =  main_set["printOnScreen"]
toRun_notebook = main_set["pipeline"][0]["name"]
timeout = main_set["pipeline"][0]["timeout"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

y1=0
y2=0

ypi = exec_set["execType"]["yearsPerIteration"]
dt1 = datetime.now()

if(exec_set["execType"]["type"]=="all"):
    y1 = exec_set["execType"]["from"]
    y2 = exec_set["execType"]["to"] + 1

if(exec_set["execType"]["type"]=="lastY"):
    y2 = datetime.now().year
    y1 = y2 - exec_set["execType"]["distance"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sys.path.append("/lakehouse/default/Files/Global_Libs")
from dap_ELogger import ELogger

#  workspace, logLevel = 2, correlation_id="", log_path=None,spark=None,printOnScreen=False
elogger = ELogger(workspace=ws,pipeline_id=pipeline_id,group=group,nb=nb ,logLevel=logLevel,printOnScreen=printOnScreen)
elogger.log_event_start(message= f"group:{group} de {y1} a {y2}")




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==============================================
# WDI Parallel Ingestion (Year-by-Year, country=all) → Bronze
# ==============================================


# ------------------------------
# Helpers de URL e HTTP
# ------------------------------
def _build_year_url(
    base_url: str,
    indicator: str,
    year: int,
    include_footnote: bool = True,
    include_scale: bool = True,
    include_ctrycode: bool = True,
    per_page: int = 20000
) -> str:
    """
    country=all fixo (não parametrizamos país).
    """
    base = f"{base_url}{indicator}"
    params = {
        "format": "json",
        "date": f"{year}",
        "per_page": str(per_page)
    }
    if include_footnote:
        params["footnote"] = "y"
    if include_scale:
        params["scale"] = "y"
    if include_ctrycode:
        params["ctrycode"] = "y"
    q = urlencode(sorted(params.items()), doseq=True)
    return f"{base}?{q}"

def _set_query_param(url: str, **params) -> str:
    u = urlparse(url)
    q = dict(parse_qsl(u.query, keep_blank_values=True))
    for k, v in params.items():
        if v is None and k in q:
            q.pop(k, None)
        elif v is not None:
            q[k] = str(v)
    new_q = urlencode(sorted(q.items()), doseq=True)
    return urlunparse((u.scheme, u.netloc, u.path, u.params, new_q, u.fragment))

def _normalize_for_signature(url: str) -> str:
    """
    Assinatura estável do URL base (sem 'page') para idempotência.
    """
    u = urlparse(url)
    q = dict(parse_qsl(u.query, keep_blank_values=True))
    q.pop("page", None)
    new_q = urlencode(sorted(q.items()), doseq=True)
    return urlunparse((u.scheme, u.netloc, u.path, u.params, new_q, u.fragment))

def _extract_indicator_code(url: str) -> str:
    path = urlparse(url).path.strip("/").split("/")
    try:
        idx = path.index("indicator")
        return path[idx + 1]
    except Exception:
        return "UNKNOWN"

def _http_get_json(url: str, timeout=180, max_retries=4, backoff_base=1.5):
    """
    GET com retries/backoff para 429/5xx.
    """
    last_err = None
    for attempt in range(max_retries + 1):
        try:
            resp = requests.get(url, timeout=timeout)
            if resp.status_code in (429, 500, 502, 503, 504):
                print(f"erro http {resp.status_code}")
                raise requests.HTTPError(f"Transient error {resp.status_code} attempt {attempt}")
            resp.raise_for_status()
            return resp.status_code, resp.json()
        except Exception as ex:
            last_err = ex
            if attempt >= max_retries:
                raise
            print(f"attempt:{attempt}")
            sleep_sec = (backoff_base ** attempt) + 0.5 * attempt
            time.sleep(sleep_sec)
    raise last_err

# ------------------------------
# Worker: faz 1 (indicador, ano)
# ------------------------------
def _fetch_one_indicator_year(
    url: str,
    indicator: str,
    year: int,
    include_footnote: bool = True,
    include_scale: bool = True,
    include_ctrycode: bool = True,
    per_page: int = 20000,
    timeout_sec: int = 180,
    max_retries: int = 4,
    backoff_base: int = 2,
    logRef: str = ""
) -> List[Dict[str, Any]]:
    """
    Faz todas as páginas para (indicator, year) e devolve rows (uma por página),
    contendo o JSON CRU de cada página e metadados para Bronze.
    """
    
    logr=json.loads(logRef)
    elogger = ELogger(correlation=logr)

    rows: List[Dict[str, Any]] = []

    base_url = _build_year_url(
        base_url=url,
        indicator=indicator,
        year=year,
        include_footnote=include_footnote,
        include_scale=include_scale,
        include_ctrycode=include_ctrycode,
        per_page=per_page
    )
    indicator_code = _extract_indicator_code(base_url)
    url_sig = _normalize_for_signature(base_url)
    query_signature = hashlib.sha256(url_sig.encode("utf-8")).hexdigest()

    # 1) Primeira página: descobrir nº de páginas
    try:
        status, payload = _http_get_json(_set_query_param(base_url, page=1),
                                     timeout=timeout_sec, max_retries=max_retries,backoff_base=backoff_base)
    except Exception as ex:
        elogger.log_event_error( message = f"First Call Indicator:{indicator} {str(ex)}")
        return rows

    total_pages = 1
    if isinstance(payload, list) and len(payload) > 0 and isinstance(payload[0], dict):
        total_pages = int(payload[0].get("pages", 1))  # meta.pages

    # 2) Iterar páginas
    for p in range(1, total_pages + 1):
        page_url = _set_query_param(base_url, page=p)

        try:
            st, page_payload = _http_get_json(page_url, timeout=timeout_sec, max_retries=max_retries,backoff_base=backoff_base)
        except Exception as ex:
            elogger.log_event_error( message = f"First Call Indicator:{indicator} {str(ex)}")
            return rows

        rows.append({
            "indicator_code": indicator_code,
            "request_year": int(year),
            "api_url": page_url,                     # URL exato usado
            "query_signature": query_signature,      # URL base sem 'page'
            "page": p,
            "per_page": per_page,
            "total_pages": total_pages,
            "http_status": st,
            "response_json": json.dumps(page_payload),
            "ingestion_ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        })

    #print(f"indicator {indicator}    year {year}    pages {total_pages}   rows per page {per_page} ")
    return rows

# ------------------------------
# Função principal (paralela)
# ------------------------------
def wdi_ingest_parallel_years_all_countries(
    url: str , 
    indicators: List[str],
    years: List[int],
    include_footnote: bool = True,
    include_scale: bool = True,
    include_ctrycode: bool = True,
    per_page: int = 20000,
    max_workers: int = 8,
    table_name: str = "wdi_bronze",
    partition_cols: Optional[List[str]] = None,   # default: ["indicator_code"]
    timeout_sec: int = 180,
    max_retries: int = 4,
    backoff_base: int = 2,
    logRef=""
) -> Dict[str, Any]:
    """
    Ingestão paralela de N indicadores × M anos (ano-a-ano) para Bronze (RAW por página),
    com country=all fixo (não parametriza países).
    """
    from pyspark.sql import types as T

    if partition_cols is None:
        partition_cols = ["indicator_code"]

    dt1 = datetime.now()
    work = [(ind, yr) for ind in indicators for yr in years]

    all_rows: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [
            ex.submit(
                _fetch_one_indicator_year,
                url=url,
                indicator=ind,
                year=yr,
                include_footnote=include_footnote,
                include_scale=include_scale,
                include_ctrycode=include_ctrycode,
                per_page=per_page,
                timeout_sec=timeout_sec,
                max_retries=max_retries,
                backoff_base=backoff_base,
                logRef=logRef
            )
            for ind, yr in work
        ]
        for fut in as_completed(futures):
            rr = fut.result()
            # print(f"##### data returned {len(rr)}")
            if len(rr) > 0:
                all_rows.extend(fut.result()) 

    # Esquema Bronze 
    schema = T.StructType([
        T.StructField("indicator_code", T.StringType(), True),
        T.StructField("request_year", T.IntegerType(), True),
        T.StructField("api_url", T.StringType(), True),
        T.StructField("query_signature", T.StringType(), True),
        T.StructField("page", T.IntegerType(), True),
        T.StructField("per_page", T.IntegerType(), True),
        T.StructField("total_pages", T.IntegerType(), True),
        T.StructField("http_status", T.IntegerType(), True),
        T.StructField("response_json", T.StringType(), True),
        T.StructField("ingestion_ts", T.StringType(), True),
    ])

    df_all = spark.createDataFrame(all_rows, schema=schema)

    # df_all = df_all.dropDuplicates(["query_signature", "page"])

   
    # 1) Lista de pares (indicador, ano) a “refreshar”
    pairs = [
        (r["indicator_code"], r["request_year"])
        for r in df_all.select("indicator_code", "request_year").distinct().collect()
    ]

    # 2) Para cada par, fazemos OVERWRITE seletivo (replaceWhere)
    tR = (datetime.now() - dt1).seconds
    dt2 = datetime.now()
    total_pages_written = 0
    for ind, yr in pairs:
        df_pair = df_all.where((col("indicator_code") == ind) & (col("request_year") == yr))
        if df_pair.count() > 0:
        # print(df_pair.count())
            # overwrite atómico só para este (indicador, ano)
            (
                df_pair.write
                    .format("delta")
                    .mode("overwrite")
                    .option("replaceWhere", f"indicator_code = '{ind}' AND request_year = {yr}")
                    # .partitionBy(*partition_cols)  # NÃO é necessário repetir; a tabela já tem partição
                    # .saveAsTable(table_name)         # <- usa saveAsTable p/ tabela do metastore
                    .save(table_name)               # <- se 'table_name' for um PATH Delta, usa esta linha
            )

        total_pages_written += df_pair.count()
    
    tW =(datetime.now() - dt2).seconds
    return {"indicators": len(indicators),"years": len(years),"rows_written": total_pages_written},tR,tW,total_pages_written  # ,"table": table_name ,"partition_cols": partition_cols,

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

 # ⚠️ Garante que a TABELA (ou o PATH) já existe antes do primeiro replaceWhere.
    #    - Se 'table_name' é um PATH (ex.: 'Files/WDI/bronze/wdi_bronze'), cria uma 1ª vez com append vazio
    #    - Se 'table_name' é uma TABELA do metastore, preferível usar saveAsTable na criação inicial.
    # Exemplo simples: criar a tabela se ainda não existir (metastore):
"""
spark.sql("
    CREATE TABLE IF NOT EXISTS facts.wdi  (
        indicator_code   STRING,
        request_year     INT,
        api_url          STRING,
        query_signature  STRING,
        page             INT,
        per_page         INT,
        total_pages      INT,
        http_status      INT,
        response_json    STRING,
        ingestion_ts     STRING
    )
    USING DELTA
    PARTITIONED BY (request_year)
")
"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# funcao que tem as chmadas para ir buscar os indicadores e os anos 

#indicators = ["SP.POP.TOTL", "NY.GDP.MKTP.CD"]
#years = list(range(1981, 1990))  # 2018, 2019, 202

def getData(indicators,years,logRef):
    summary = wdi_ingest_parallel_years_all_countries(
        url=url,
        indicators=indicators,
        years=years,
        include_footnote=True,
        include_scale=False,
        include_ctrycode=True,
        per_page=per_page,            # ajuda a reduzir paginação
        max_workers=max_workers,             # ajusta consoante o load e 429s
        table_name=indicators_path,
        max_retries=url_call_max_retries,
        backoff_base=url_call_base_delay,
        logRef=logRef
    )
    return summary

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
tot_indicators = len(indicators)

elogger.log_event_info(step_name = f"10 - end get identify indicators", message = f"get indicators to run. group:{group}  total {tot_indicators}")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# main

#indicators = ["SP.POP.TOTL", "NY.GDP.MKTP.CD"]


ttR = 0
ttW = 0
total_pages_written = 0
for i in range(0, len(indicators), indicators_iteration_size): 
    dtt = datetime.now() 
    sub_list = indicators[i:i+3]
    elogger.log_event_info(step_name = "15 - run_indicators", message = f"start proc for {sub_list}  iteracao:{i}/{tot_indicators}", logLevel= 3)
    logRef = json.dumps(elogger.get_correlation())

    for ano in range(y1, y2, ypi):
        years = list(range(ano, ano + ypi))
        dtts = (datetime.now() - dtt).seconds
        dtt = datetime.now() 
        print(f"last:{dtts}  pages:{total_pages_written}   next:{years}    {datetime.now()}")
        s,tR,tW,pages_written =getData(sub_list,years,logRef)
        ttR = ttR + tR
        ttW = ttW + tW
        total_pages_written=total_pages_written+pages_written
        #print(s)

elogger.log_event_info(step_name = "20 - end get group indicators from api", message = f"time calls API {ttR}s     time write data lake {ttW}      indicators {len(indicators)}       total pages written {total_pages_written}")

elogger.log_event_Finish(message= f"")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select count(distinct indicator_code) from facts.wdi

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
