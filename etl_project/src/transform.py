from pyspark.sql import functions as F
from pyspark.sql import types as T

def transform_data(df_data):
    # Data Cleaning
    df_drop_nulls = df_data.dropna(subset=['fare_amount','tpep_dropoff_datetime','tpep_pickup_datetime','trip_distance'])
    df_filter_data = df_drop_nulls.filter((F.col("fare_amount") > 0) & (F.col("trip_distance") > 0))

    df_cleaned = df_filter_data.withColumn("tpep_pickup_datetime", F.to_timestamp("tpep_pickup_datetime"))\
                                        .withColumn("tpep_dropoff_datetime",F.to_timestamp("tpep_dropoff_datetime"))\
                                        .withColumn("trip_distance", F.col("trip_distance").cast(T.FloatType()))\
                                        .withColumn("total_amount", F.col("total_amount").cast(T.FloatType()))\
                                        .withColumn("tolls_amount", F.col("tolls_amount").cast(T.FloatType()))\
                                        .withColumn("tip_amount", F.col("tip_amount").cast(T.FloatType()))\
                                        .withColumn("mta_tax", F.col("mta_tax").cast(T.FloatType()))\
                                        .withColumn("fare_amount", F.col("fare_amount").cast(T.FloatType()))\
                                        .withColumn("passenger_count", F.col("passenger_count").cast(T.IntegerType()))
    
    # feature Engineering
    df = df_cleaned.withColumn("trip_duration_mins", F.round((F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime"))/60))
    df = df.withColumn("distance_category", F.when(F.col("trip_distance") < 2, "Short")
                                                .when((F.col("trip_distance") >=2) & (F.col("trip_distance") <=5), "Medium")
                                                .otherwise("Long"))
    df = df.withColumn("trip_revenue", F.col("total_amount") - F.col("tip_amount"))

    # Business Logics
    #low_tip_flag
    df = df.withColumn("low_tip_flag", F.when(F.col("tip_amount") < 1, True).otherwise(False))
    #Cal revenue per passenger
    df = df.withColumn("revenue_per_passenger", F.when(F.col("passenger_count")>0, F.col("trip_revenue") / F.col("passenger_count")).otherwise(0))
    #Peak Hours
    df = df.withColumn("hour_of_day", F.hour("tpep_pickup_datetime"))
    df = df.withColumn("is_peak_hour",F.when((F.col("hour_of_day").between(7,9)) | (F.col("hour_of_day").between(17,19)), True).otherwise(False))

    df = df.withColumn("distance_efficiency", F.col("trip_distance")/F.col("trip_duration_mins"))
    df = df.withColumn("inefficient_route", F.when(F.col("distance_efficiency") < 0.05, True).otherwise(False))

    df_high_fare_zone = df.groupBy("pulocationid").agg(F.avg("trip_revenue").alias("avg_revenue_per_zone"))
    df_high_fare_zone = df_high_fare_zone.filter(F.col("avg_revenue_per_zone") > 10)
    df = df.join(df_high_fare_zone.select("pulocationid"),"pulocationid", "leftsemi").withColumn("high_revenue_zone", F.lit(True))

    #aggregrations
    df = df.withColumn("month", F.month("tpep_pickup_datetime"))
    monthly_agg = df.groupBy("pulocationid","month").agg(F.sum("fare_amount").alias("Total_fare_monthly"),
                                                         F.avg("fare_amount").alias("avg_fare_monthly"),
                                                         F.count("fare_amount").alias("count_fare_monthly"))

    df = df.join(monthly_agg, ["pulocationid","month"], "left")
    #final output
    final_df = df.select("vendorid", "tpep_pickup_datetime", "tpep_dropoff_datetime", "pulocationid", "dolocationid",
        "fare_amount", "trip_distance", "trip_duration_mins", "distance_category", "trip_revenue",
        "low_tip_flag", "is_peak_hour", "revenue_per_passenger", "inefficient_route",
        "high_revenue_zone", "month", "Total_fare_monthly", "avg_fare_monthly", "count_fare_monthly")
    
    return final_df                                 