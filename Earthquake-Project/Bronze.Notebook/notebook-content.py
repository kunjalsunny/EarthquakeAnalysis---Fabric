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
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
import requests
import json
import os
from datetime import date, timedelta
from notebookutils import mssparkutils


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************



start_date = date.today() - timedelta(7)
end_date = date.today() - timedelta(1)

output = json.dumps({
    "start_date": str(start_date),
    "end_date": str(end_date)
})

mssparkutils.notebook.exit(output)


url = f"https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={start_date}&endtime={end_date}"

try:
    response = requests.get(url)

    response.raise_for_status()

    data = response.json().get('features',[])

    if not data:
        print(f"No data available")
    else: 
        lakehouse_path = "abfss://589b70fb-043e-4206-89a2-1c6db26bd3f3@onelake.dfs.fabric.microsoft.com/2cc7bc98-d1ed-488b-8aae-8bdd53004a21"
        
        print(lakehouse_path)
        file_path = f"{lakehouse_path}/Files/{start_date}_earthquake.json"
        mssparkutils.fs.put(file_path, json.dumps(data, indent=4), True)

        # with open(file_path,'w') as file:
        #     json.dump(data,file,indent=4)
        
        print(f'Data successfully saved at: {file_path}')

except requests.exceptions.RequestException as e:
    print(f"Error fetching api: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(mssparkutils.fs.ls('Files/'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.option("multiline", "true").json("Files/2026-03-05_earthquake.json")
# df now is a Spark DataFrame containing JSON data from "Files/2026-03-05_earthquake.json".
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
