from prefect import Flow, task
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.extract import extract_data
from src.transform import transform_data
from src.load import load_data
from src.notify import send_email

from config import dev_config
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

@task
def initialize_spark():
    return SparkSession.builder\
        .appName(dev_config["spark_app_name"])\
        .master(dev_config["spark_master"])\
        .getOrCreate()

@task
def run_extract(spark):
    data = extract_data(spark)
    if data is None or data.isEmpty():
        raise ValueError("No data is received")
    return data

@task
def run_transformation(data):
    data_tranform = transform_data(data)
    return data_tranform

@task
def run_load(data_tranform):
    load_data(data_tranform)

@task
def send_success_notification():
    send_email("ETL Job Success", " ETL job completed")

@task
def send_failure_notification():
    send_email("ETL Job Failed", " ETL job has some errors")

with Flow("ET_Flow") as flow:
    spark = initialize_spark()
    try:
        extracted_data = run_extract(spark)
        transformed_data = run_transformation(extracted_data)
        run_load(transformed_data)
        send_success_notification()
    except Exception as e:
        send_failure_notification()
    finally:
        spark.stop()

if __name__ == "__main__":
    flow.run()





