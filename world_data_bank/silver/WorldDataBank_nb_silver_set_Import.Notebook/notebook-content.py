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
# MAGIC with x as(
# MAGIC SELECT
# MAGIC     *
# MAGIC FROM
# MAGIC     dims.topics t
# MAGIC     LEFT JOIN dims.indicators_topic it ON t.topic_id = it.topic_id
# MAGIC     LEFT JOIN dims.indicators i ON it.indicator_id = i.indicator_id)
# MAGIC 
# MAGIC select * from x where indicator_id_original = 'SP.POP.TOTL'
# MAGIC 
# MAGIC --select topic_name,topic_id, count(*) from x group by topic_name,topic_id order by 3 desc

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def setImportTopicsToRun(updateValue,where,fr,to,lbl=""):

    sql = f"""
    MERGE INTO dims.indicators AS i
    USING (
        select id FROM
            (SELECT it.indicator_id id,  ROW_NUMBER() OVER (ORDER BY it.indicator_id) rn
            FROM dims.indicators_topic it LEFT JOIN  dims.topics t  ON t.topic_id = it.topic_id  LEFT JOIN  dims.indicators i  ON i.indicator_id = it.indicator_id
            WHERE {where}
            group by it.indicator_id)
            WHERE rn between {fr} and {to}
    ) AS t2
    ON i.indicator_id = t2.id 
    WHEN MATCHED THEN
    UPDATE SET toImport = {updateValue};
    """

    df = spark.sql(sql)

    rows_change = df.select("num_affected_rows").first()[0]
    rows_updated = df.select("num_updated_rows").first()[0]
    rows_deleted = df.select("num_deleted_rows").first()[0]
    rows_inserted = df.select("num_inserted_rows").first()[0]

    if lbl == "":
        lbl = where 

    print(f" affected:{rows_change} i:{rows_inserted} u:{rows_updated} d:{rows_deleted} - {lbl}")

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = setImportTopicsToRun(0,"1 = 1",1,1000000,"reset")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = setImportTopicsToRun(0,"1 = 1",1,1000000,"reset")
df = setImportTopicsToRun(1,"t.topic_name = 'Science & Technology' and i.toImport = 0",1,10000,"Science & Technology")
df = setImportTopicsToRun(2,"t.topic_name = 'Health'  and i.toImport = 0 ",1,100,"Health, 2  ")
df = setImportTopicsToRun(3,"t.topic_name = 'Health'  and i.toImport = 0 ",1,100,"Health, 3  ")
df = setImportTopicsToRun(4,"t.topic_name = 'Health'  and i.toImport = 0 ",1,100,"Health, 4  ")
df = setImportTopicsToRun(5,"t.topic_name = 'Health'  and i.toImport = 0 ",1,100,"Health, 5  ")
df = setImportTopicsToRun(6,"t.topic_name = 'Health'  and i.toImport = 0 ",1,50,"Health, 6  ")
df = setImportTopicsToRun(7,"t.topic_name = 'Health'  and i.toImport = 0 ",1,50,"Health, 7  ")
df = setImportTopicsToRun(8,"t.topic_name = 'Health'  and i.toImport = 0 ",1,50,"Health, 8 ")
df = setImportTopicsToRun(9,"t.topic_name = 'Health'  and i.toImport = 0 ",1,100,"Health, 9  ")
df = setImportTopicsToRun(10,"t.topic_name = 'Health'  and i.toImport = 0 ",1,100000,"Health, 10  ")

# df = setImportTopicsToRun(3,"t.topic_name = 'Health'  and i.toImport = 0 and it.indicator_id >= 'sh.n' ","Health, >= 'sh.n'")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select id FROM
# MAGIC     (SELECT it.indicator_id id,  ROW_NUMBER() OVER (ORDER BY it.indicator_id) rn
# MAGIC     FROM dims.indicators_topic it LEFT JOIN  dims.topics t  ON t.topic_id = it.topic_id  LEFT JOIN  dims.indicators i  ON i.indicator_id = it.indicator_id
# MAGIC     WHERE t.topic_name = 'Health'   and i.toImport = 0
# MAGIC     group by it.indicator_id)
# MAGIC     WHERE rn between 200 and 300
# MAGIC order by id asc

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC SELECT t.topic_name,  count(*) ct
# MAGIC     FROM dims.indicators_topic it LEFT JOIN  dims.topics t  ON t.topic_id = it.topic_id  LEFT JOIN  dims.indicators i  ON i.indicator_id = it.indicator_id
# MAGIC     --WHERE t.topic_name = 'Health'   and i.toImport = 0
# MAGIC     group by t.topic_name
# MAGIC     order by ct desc

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC select count(*) from facts.wdi_extras

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
