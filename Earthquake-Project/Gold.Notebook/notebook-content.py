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
from pyspark.sql.functions import col, when, udf
from pyspark.sql.types import StringType
import reverse_geocoder as rg

from datetime import date, timedelta

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

start_date = date.today() - timedelta(7)

df = spark.read.table("earthquake_events_silver").filter(col('time')>start_date)
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_country_code(lat, lon):
    coordinates = (float(lat), float(lon))
    return rg.search(coordinates)[0].get('cc')

# get_country_code(48.8588443, 2.2943506)

get_country_code_udf = udf(get_country_code,StringType())

df_with_location = \
    df \
    .withColumn("Country_code", get_country_code_udf(col("latitude"),col("longitude"))) 


df_with_location_sig_class = \
    df_with_location \
    .withColumn('Sig_class', when(col('sig') < 100, "Low") \
                .when((col('sig')>=100) & (col('sig')<500), "Moderate") \
                .otherwise("High") \
)

df_with_location_sig_class.write.mode('append').saveAsTable('earthquake_events_gold')

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
