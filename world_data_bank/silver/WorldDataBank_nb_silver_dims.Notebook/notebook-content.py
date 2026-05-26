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
# META         }
# META       ]
# META     }
# META   }
# META }

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
lh_silver = exec_set["lh_silver"]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Indicadores ____

# CELL ********************

from pyspark.sql.functions import explode, col,lower,regexp_replace,lit,trim

indicators_path_raw = f"{lh_bronze}/Tables/dims/indicators_catalog"
df = spark.read.format("delta") \
    .load(indicators_path_raw) \
    .withColumn("active", lit(1)) \
    .withColumn("indicator_id", lower("id")) \
    .withColumn("indicator_id_original", col("id")) 

df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_explode_topics = (
    df.select(
        "indicator_id",
        "its",
        "active",
        explode("topics").alias("topic"))
)

df_explode_topics_flat = (
    df_explode_topics
    .select(
        "indicator_id",
        "active",
        lower(col("topic.id")).alias("topic_id"),
        trim(col("topic.value")).alias("topic_name"),
        "its"
    )
).distinct()

df_explode_topics_dim = df_explode_topics_flat.select("topic_id", "topic_name","its","active").distinct()

df_topics = df_explode_topics_dim.withColumn("topic",
    lower(
        regexp_replace(
            regexp_replace(col("topic_name"), "[^A-Za-z0-9 ]", ""),
            " ",
            "_"
        )
    )
)

df_explode_topics_dim = df_explode_topics_flat \
    .select("indicator_id","topic_id","its","active") \
    .distinct() \
    .filter("topic_id IS NOT NULL and topic_id != 'NULL'")


df_sources = (
    df
    .select(
        "indicator_id",
        lower(col("`source.id`")).alias("source_id"),
        col("`source.value`").alias("source_value"),
        "sourceNote","sourceOrganization","its","active"
    )
)

df_sources_dim = df_sources.select("source_id", "source_value","sourceNote","sourceOrganization","its","active").distinct()

df_indicators_source_dim = df_sources.select("source_id", "indicator_id","its","active").distinct()

df_indicator =  (
    df.select(
        "indicator_id",
        "indicator_id_original",
        "name",
        "unit",
        "its",
        "active")
).withColumn("toImport", lit(0)) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

"""
# PARA SER EXECUTADO DA 1a vez

topics_path = f"{lh_silver}/Tables/dims/topics"
df_topics_1st = df_topics.limit(0)
df_topics_1st.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(topics_path)

indicators_topic_path = f"{lh_silver}/Tables/dims/indicators_topic"
dim_indicators_topic_1st = df_explode_topics_dim.limit(0)
dim_indicators_topic_1st.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(indicators_topic_path)

indicators_source_path = f"{lh_silver}/Tables/dims/indicators_source"
df_indicators_source_dim_1st = df_indicators_source_dim.limit(0)
df_indicators_source_dim_1st.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(indicators_source_path)

sources_path = f"{lh_silver}/Tables/dims/sources"
df_sources_dim_1st = df_sources_dim.limit(0)
df_sources_dim_1st.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(sources_path)


indicators_path = f"{lh_silver}/Tables/dims/indicators"
df_indicator_1st = df_indicator.limit(0)
df_indicator_1st.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(indicators_path)
"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_topics.createOrReplaceTempView("stg_topics")
df_explode_topics_dim.createOrReplaceTempView("stg_indicators_topic")
df_sources_dim.createOrReplaceTempView("stg_sources")
df_indicators_source_dim.createOrReplaceTempView("stg_indicators_source")
df_indicator.createOrReplaceTempView("stg_df_indicator")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

indicators_path = f"{lh_silver}/Tables/dims/indicators"
df_indicator_1st = df_indicator.limit(0)
df_indicator_1st.write \
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

# MAGIC %%sql
# MAGIC -- topics 
# MAGIC 
# MAGIC WITH topics1 as (
# MAGIC     select 
# MAGIC     topic_id,topic_name,its,topic,active 
# MAGIC     ,ROW_NUMBER() OVER (PARTITION BY topic_id ORDER BY 1) AS rn
# MAGIC     from stg_topics where topic_id is not null and topic_id != ''
# MAGIC ),
# MAGIC topics2 as (select topic_id,topic_name,its,topic,active from topics1 where rn = 1)
# MAGIC 
# MAGIC MERGE INTO  dims.topics AS tgt
# MAGIC USING topics2 AS src
# MAGIC ON tgt.topic_id = src.topic_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     tgt.topic_name = src.topic_name,
# MAGIC     tgt.its = src.its,
# MAGIC     tgt.topic = src.topic,
# MAGIC     tgt.active = src.active
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (topic_id,topic_name,its,topic,active)
# MAGIC   VALUES ( topic_id,topic_name,its,topic,active)
# MAGIC WHEN NOT MATCHED BY SOURCE THEN
# MAGIC    UPDATE SET
# MAGIC     tgt.active = 0
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- indicators_topic
# MAGIC 
# MAGIC MERGE INTO  dims.indicators_topic AS tgt
# MAGIC USING stg_indicators_topic AS src
# MAGIC ON tgt.topic_id = src.topic_id and tgt.indicator_id = src.indicator_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     tgt.its = src.its,
# MAGIC     tgt.active = src.active
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (topic_id,indicator_id,its,active)
# MAGIC   VALUES (topic_id,indicator_id,its,active)
# MAGIC WHEN NOT MATCHED BY SOURCE THEN
# MAGIC    UPDATE SET
# MAGIC     tgt.active = 0
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- sources 
# MAGIC 
# MAGIC WITH sources1 as (
# MAGIC     select 
# MAGIC     source_id,source_value,sourceNote,sourceOrganization,its,active 
# MAGIC     ,ROW_NUMBER() OVER (PARTITION BY source_id ORDER BY 1) AS rn
# MAGIC     from stg_sources where source_id is not null and source_id != ''
# MAGIC ),
# MAGIC sources2 as (select source_id,source_value,sourceNote,sourceOrganization,its,active 
# MAGIC             from sources1 where rn = 1)
# MAGIC 
# MAGIC MERGE INTO  dims.sources AS tgt
# MAGIC USING sources2 AS src
# MAGIC ON tgt.source_id = src.source_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     tgt.source_value = src.source_value,
# MAGIC     tgt.its = src.its,
# MAGIC     tgt.sourceNote = src.sourceNote,
# MAGIC     tgt.sourceOrganization = src.sourceOrganization,
# MAGIC     tgt.active = src.active
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (source_id,source_value,sourceNote,sourceOrganization,its,active)
# MAGIC   VALUES (source_id,source_value,sourceNote,sourceOrganization,its,active)
# MAGIC WHEN NOT MATCHED BY SOURCE THEN
# MAGIC    UPDATE SET
# MAGIC     tgt.active = 0
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- indicators_topic
# MAGIC 
# MAGIC MERGE INTO  dims.indicators_source AS tgt
# MAGIC USING stg_indicators_source AS src
# MAGIC ON tgt.indicator_id = src.indicator_id and tgt.source_id = src.source_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     tgt.its = src.its,
# MAGIC     tgt.active = src.active
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (source_id,indicator_id,its,active)
# MAGIC   VALUES (source_id,indicator_id,its,active)
# MAGIC WHEN NOT MATCHED BY SOURCE THEN
# MAGIC    UPDATE SET
# MAGIC     tgt.active = 0
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- indicators 
# MAGIC 
# MAGIC WITH indicators1 as (
# MAGIC     select 
# MAGIC     indicator_id,name,unit,its,active,toImport,indicator_id_original
# MAGIC     ,ROW_NUMBER() OVER (PARTITION BY indicator_id ORDER BY 1) AS rn
# MAGIC     from stg_df_indicator -- where source_id is not null and source_id != ''
# MAGIC ),
# MAGIC indicators2 as (select indicator_id,name,unit,its,active,toImport,indicator_id_original
# MAGIC             from indicators1 where rn = 1)
# MAGIC 
# MAGIC MERGE INTO  dims.indicators AS tgt
# MAGIC USING indicators2 AS src
# MAGIC ON tgt.indicator_id = src.indicator_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     tgt.name = src.name,
# MAGIC     tgt.its = src.its,
# MAGIC     tgt.unit = src.unit,
# MAGIC     tgt.active = src.active,
# MAGIC     tgt.toImport = src.toImport,
# MAGIC     tgt.indicator_id_original = src.indicator_id_original
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (indicator_id,name,unit,its,active,toImport,indicator_id_original)
# MAGIC   VALUES (indicator_id,name,unit,its,active,toImport,indicator_id_original)
# MAGIC WHEN NOT MATCHED BY SOURCE THEN
# MAGIC    UPDATE SET
# MAGIC     tgt.active = 0

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC -- select topic_id,  topic_name,its,topic, active from dims.topics
# MAGIC -- select topic_id,indicator_id,its,active from dims.indicators_topic
# MAGIC -- select source_id,source_value,sourceNote,sourceOrganization,its,active from dims.sources
# MAGIC -- select source_id,indicator_id,its,active from dims.indicators_source limit 10
# MAGIC select indicator_id,name,unit,its,active,toImport,indicator_id_original from dims.indicators limit 10

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC select indicator_id,count(*) from dims.indicators
# MAGIC group by indicator_id order by 2 desc

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC MERGE INTO dims.indicators AS i
# MAGIC USING (
# MAGIC     SELECT indicator_id
# MAGIC     FROM dims.indicators_topic
# MAGIC     WHERE topic_id = 14
# MAGIC ) AS t
# MAGIC ON i.id = t.indicator_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET import = 1;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC -- #### ATTENTION: AI-generated code can include errors or operations you didn't intend. Review the code in this cell carefully before running it.
# MAGIC 
# MAGIC -- Replace column and table names if necessary!
# MAGIC -- This Spark SQL query joins dims_topics, indicators_topic, and indicators tables.
# MAGIC -- Assumed join keys: dims_topics.topic_id = indicators_topic.topic_id and indicators_topic.indicator_id = indicators.indicator_id
# MAGIC with x as(
# MAGIC SELECT
# MAGIC     t.topic_name,t.topic_id,i.name
# MAGIC FROM
# MAGIC     dims.topics t
# MAGIC     LEFT JOIN dims.indicators_topic it ON t.topic_id = it.topic_id
# MAGIC     LEFT JOIN dims.indicators i ON it.indicator_id = i.indicator_id)
# MAGIC 
# MAGIC select topic_name,topic_id, count(*) from x group by topic_name,topic_id order by 3 desc

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select * from stg_dim_indicators

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
