# Databricks notebook source

%run ./../../tools/model/extract

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, TimestampType
from pyspark.sql import DataFrame
from functools import reduce

class YellowTripData(Extract):

    @property
    def database(self):
        return "nyc_tlc"


    @property
    def layer(self):
        return "gold"    
    

    @property
    def entity(self):
        return "YellowTripdata"
    

    @property
    def source_table(self): 
        return f"frt_{self.dlm.get_region_env()}.silver.nyc_tlc__yellowtripdata"                

        
    @property
    def source(self):
        table = self.source_table
        source_df = spark.table(table) 
        if self.full_load is False:    
            if self.offset_hours > 0:
                source_df = source_df.filter(col("tpep_pickup_datetime") >= lit(str(self.target_hours)))
            else:
                source_df = source_df.filter(to_date(col("tpep_pickup_datetime")) >= self.target_date)
    
        return source_df


    @property
    def transformer(self):

        # colunas obrigatórias
        REQUIRED_COLS = [
            "vendorid",
            "passenger_count",
            "total_amount",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
        ]

        df_silver = self.source

        # Valida presença das colunas obrigatórias
        missing = [c for c in REQUIRED_COLS if c not in df_silver.columns]
        if missing:
            raise Exception(f"Colunas obrigatórias ausentes na Silver: {missing}")

        # Colunas complementares para enriquecer o consumo
        CONSUMPTION_COLS = REQUIRED_COLS + [
            "trip_distance",
            "trip_duration_minutes",
            "avg_speed_kmh",
            "fare_amount",
            "tip_amount",
            "tip_pct",
            "has_tip",
            "payment_type_desc",
            "ratecode_desc",
            "pickup_period",
            "is_weekend",
            "pickup_year",
            "pickup_month",
            "pickup_day",
            "pickup_hour",
        ]

        # Transformações e regras de qualidade para consumo
        return (
            df_silver
            .select(CONSUMPTION_COLS)

            # Garante que colunas obrigatórias não são nulas
            .filter(F.col("vendorid").isNotNull())
            .filter(F.col("passenger_count").isNotNull())
            .filter(F.col("total_amount").isNotNull())
            .filter(F.col("tpep_pickup_datetime").isNotNull())
            .filter(F.col("tpep_dropoff_datetime").isNotNull())

            # Renomeia para padrão de negócio (amigável para usuários)
            .withColumnRenamed("vendorid",             "vendor_id")
            .withColumnRenamed("pulocationid",         "pickup_location_id")
            .withColumnRenamed("dolocationid",         "dropoff_location_id")
            .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
            .withColumnRenamed("tpep_dropoff_datetime","dropoff_datetime")

            # Auditoria gold
            .withColumn("_gold_timestamp", F.current_timestamp())
            .withColumn("_gold_pipeline",  F.lit("nyc_tlc__yellowtripdata_gold"))
        )


    @property
    def partitions(self):
        return ["pickup_year", "pickup_month", "vendor_id"]


    @property
    def merge_key(self):
        return ["vendor_id", "pickup_datetime", "pickup_location_id", "dropoff_location_id"]

# COMMAND ----------

full_load = Widget.get_or_else("full_load", True)
YellowTripData(full_load=full_load, debug=True).run(operation="overwrite")
