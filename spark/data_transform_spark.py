import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import argparse

parser = argparse.ArgumentParser()
# parser.add_argument('--temp_bucket', required=True)
parser.add_argument('--dataset', required=True)
args = parser.parse_args()

spark = SparkSession.builder \
    .appName('weather_aggregating') \
    .getOrCreate()

# spark.conf.set('temporaryGcsBucket', args.temp_bucket)
spark.conf.set("viewsEnabled", "true")
spark.conf.set("materializationDataset", args.dataset)

# data = [("James","","Smith","36636","M",3000),
#     ("Michael","Rose","","40288","M",4000)]
# df = spark.createDataFrame(data=data)
# df.show()

# data = spark.read.format('bigquery') \
#   .option('table', args.dataset+'.Ankara_data') \
#   .load()
sql = f"""
  WITH union_data AS (
    SELECT *, date_trunc(date_time, month) AS month_1st
      FROM de_project_dataset.Ankara_data
    UNION ALL
    SELECT *, date_trunc(date_time, month) AS month_1st
      FROM de_project_dataset.Istanbul_data
    UNION ALL
    SELECT *, date_trunc(date_time, month) AS month_1st
      FROM de_project_dataset.Antalya_data    
  ), for_mode AS (
    SELECT address, month_1st, conditions, 
      COUNT(conditions) AS freq,
      RANK() OVER (PARTITION BY address, month_1st ORDER BY COUNT(conditions) DESC) AS r
      FROM union_data
      GROUP BY address, month_1st, conditions
  )    
  SELECT union_data.address AS city,
    union_data.month_1st,
    AVG(union_data.temperature) AS temperature,
    AVG(union_data.relative_humidity) AS relative_humidity,
    AVG(union_data.wind_speed) AS wind_speed,
    AVG(union_data.precipitation) AS precipitation,
    AVG(union_data.sea_level_pressure) AS sea_level_pressure,
    for_mode.conditions
  FROM union_data INNER JOIN for_mode
  ON union_data.address = for_mode.address AND union_data.month_1st = for_mode.month_1st
  WHERE for_mode.r = 1
  GROUP BY city, month_1st, conditions
"""
data = spark.read.format("bigquery").load(sql)  

data.write.format('bigquery') \
    .option('writeMethod', 'direct') \
    .mode('overwrite') \
    .save(args.dataset+'.summary')