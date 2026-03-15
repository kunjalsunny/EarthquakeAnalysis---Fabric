# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2cc7bc98-d1ed-488b-8aae-8bdd53004a21",
# META       "default_lakehouse_name": "Earthquake_lakehouse",
# META       "default_lakehouse_workspace_id": "589b70fb-043e-4206-89a2-1c6db26bd3f3",
# META       "known_lakehouses": [
# META         {
# META           "id": "2cc7bc98-d1ed-488b-8aae-8bdd53004a21"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "2e04af38-b5f7-952c-415d-c048a2f093ee",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
from pyspark.sql.functions import col, isnull, when
from pyspark.sql.types import TimestampType
from datetime import date, timedelta

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

start_date = date.today() - timedelta(7)

df = spark.read.option("multiline",'true').json(f"Files/{start_date}_earthquake.json")
df = (
    df.select(
        "id",
        col("geometry.coordinates").getItem(0).alias('longitude'),
        col("geometry.coordinates").getItem(1).alias("latitude"),
        col("geometry.coordinates").getItem(2).alias("elevation"),
        col("properties.title").alias("Title"),
        col("properties.place").alias("sig"),
        col("properties.mag").alias("mag"),
        col('properties.magType').alias('magType'),
        col('properties.time').alias('time'),
        col('properties.updated').alias('updated')
    )
)
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = (
    df
    .withColumn('longitude',when(isnull(col('longitude')), 0).otherwise(col('longitude')))
    .withColumn('latitude',when(isnull(col('latitude')), 0).otherwise(col('latitude')))
    .withColumn('time',when(isnull(col('time')), 0).otherwise(col('time')))
)
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = (
    df
    .withColumn('time', (col('time')/ 1000).cast(TimestampType()))
    .withColumn('updated',(col('updated')/1000).cast(TimestampType()))
)
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.write.mode('append').saveAsTable('earthquake_events_silver')

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
