import requests
import json

from pyspark.sql import SparkSession
from config import config_dev

def extract_data(spark: SparkSession):
    url = config_dev["data_source"]
    response = requests.get(url)
    json_data = response.text
    df_data = spark.read.json(spark.sparkContext.parallelize([json_data]))
    print(df_data.count())
    return df_data