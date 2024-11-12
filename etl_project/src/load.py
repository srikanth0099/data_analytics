import psycopg2
from config import config_dev

def load_data(data_final):
    db_url = config_dev["postgres_connection_string"]
    user = config_dev["db_user"]
    pwd = config_dev["db_pwd"]

    #data_final.write.mode("overwrite").option("header","true").csv("dbfs:/FileStore/data/taxi.csv")

    try:
        data_final.write.format("jdbc")\
                    .option("url", db_url)\
                    .option("dbtable","taxi_details")\
                    .option("user",user)\
                    .option("password", pwd)\
                    .mode("append")\
                    .save()
        print("Data Sucessfully written into Postgres Database")
    except Exception as e:
        print("There is an Error :",e)
    
