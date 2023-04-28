import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--temp_bucket', required=True)
parser.add_argument('--dataset', required=True)
args = parser.parse_args()

spark = SparkSession.builder \
    .appName('weather_aggregating') \
    .getOrCreate()

spark.conf.set('temporaryGcsBucket', args.temp_bucket)
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
    SELECT * FROM {args.dataset}.Ankara_data
    union all
    SELECT * FROM {args.dataset}.Istanbul_data
    union all
    SELECT * FROM {args.dataset}.Antalya_data    
  )
  SELECT address AS city,
    date_trunc(date_time, month) AS month,
    AVG(temperature) as temperature,
    AVG(relative_humidity) as relative_humidity,
    AVG(wind_speed) as wind_speed,
    AVG(precipitation) as precipitation,
    AVG(sea_level_pressure) as sea_level_pressure
  FROM union_data
  GROUP BY city, month
  """
data = spark.read.format("bigquery").load(sql)  

data.write.format('bigquery') \
    .option('table', args.dataset+'.summary') \
    .mode('overwrite') \
    .save()