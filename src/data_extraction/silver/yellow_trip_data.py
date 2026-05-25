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
        return "silver"    
    

    @property
    def entity(self):
        return "YellowTripdata"
    

    @property
    def source_table(self):
        return f"frt_{self.dlm.get_region_env()}.bronze.nyc_tlc__yellowtripdata"                

        
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

        # Leitura da Bronze
        df_bronze = self.source

        # Deduplicação
        df_dedup = (
            df_bronze
            .dropDuplicates(["vendorid", "tpep_pickup_datetime"])
        )

        # Limpeza e validações de qualidade
        df_clean = (
            df_dedup

            # Sanidade temporal
            .withColumn("_file_year",  F.regexp_extract("_src_file_name", r"(\d{4})-\d{2}\.parquet$", 1).cast("int"))
            .withColumn("_file_month", F.regexp_extract("_src_file_name", r"\d{4}-(\d{2})\.parquet$", 1).cast("int"))
            .filter(
                (F.year("tpep_pickup_datetime")  == F.col("_file_year")) &
                (F.month("tpep_pickup_datetime") == F.col("_file_month"))
            )
            .drop("_file_year", "_file_month")
            

            # Remove corridas com dados fundamentais nulos
            .filter(F.col("vendorid").isNotNull())
            .filter(F.col("tpep_pickup_datetime").isNotNull())
            .filter(F.col("tpep_dropoff_datetime").isNotNull())

            # Remove corridas com tempo inválido (chegada antes da partida)
            .filter(F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"))

            # Remove corridas com valores financeiros negativos
            .filter(F.col("fare_amount")  >= 0)
            .filter(F.col("total_amount") >= 0)
            .filter(F.col("trip_distance") >= 0)

            # Remove passageiros inválidos (0 ou mais de 6)
            .filter(F.col("passenger_count").between(1, 6))

            # Remove corridas com distância zero e valor zero — provável erro de sistema
            .filter(~((F.col("trip_distance") == 0) & (F.col("fare_amount") == 0)))

            # Padroniza store_and_fwd_flag — Y/N
            .withColumn(
                "store_and_fwd_flag",
                F.when(F.upper(F.col("store_and_fwd_flag")) == "Y", F.lit("Y"))
                .when(F.upper(F.col("store_and_fwd_flag")) == "N", F.lit("N"))
                .otherwise(F.lit("N"))
            )
        )

        # Enriquecimento — novas colunas derivadas
        df_enriched = (
            df_clean

            # duração da corrida em minutos
            .withColumn(
                "trip_duration_minutes", F.round((F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60,2).cast(DoubleType())
            )

            # velocidade média (km/h) — trip_distance em milhas → converte para km
            .withColumn(
                "avg_speed_kmh",
                F.round(
                    (F.col("trip_distance") * 1.60934) /
                    F.nullif(
                        (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 3600,F.lit(0)),2).cast(DoubleType()))

            # custo por milha
            .withColumn("fare_per_mile",F.round(F.col("fare_amount") / F.nullif(F.col("trip_distance"), F.lit(0)), 2).cast(DoubleType()))

            # percentual de gorjeta sobre o valor da corrida
            .withColumn("tip_pct",F.round(F.col("tip_amount") / F.nullif(F.col("fare_amount"), F.lit(0)) * 100,2).cast(DoubleType())
            )

            # flag de gorjeta paga
            .withColumn("has_tip", F.when(F.col("tip_amount") > 0, True).otherwise(False))

            # Período do dia da corrida
            .withColumn(
                "pickup_period",
                F.when(F.hour("tpep_pickup_datetime").between(0,  5),  "madrugada")
                .when(F.hour("tpep_pickup_datetime").between(6,  11), "manha")
                .when(F.hour("tpep_pickup_datetime").between(12, 17), "tarde")
                .when(F.hour("tpep_pickup_datetime").between(18, 23), "noite")
            )

            # flag fim de semana
            .withColumn("is_weekend",F.dayofweek("tpep_pickup_datetime").isin([1, 7])  # 1=domingo, 7=sábado)

            # partições de tempo para leitura eficiente
            .withColumn("pickup_year",  F.year("tpep_pickup_datetime").cast(LongType()))
            .withColumn("pickup_month", F.month("tpep_pickup_datetime").cast(LongType()))
            .withColumn("pickup_day",   F.dayofmonth("tpep_pickup_datetime").cast(LongType()))
            .withColumn("pickup_hour",  F.hour("tpep_pickup_datetime").cast(LongType()))

            # decode do payment_type
            .withColumn(
                "payment_type_desc",
                F.when(F.col("payment_type") == 1, "Credit Card")
                .when(F.col("payment_type") == 2, "Cash")
                .when(F.col("payment_type") == 3, "No Charge")
                .when(F.col("payment_type") == 4, "Dispute")
                .when(F.col("payment_type") == 5, "Unknown")
                .when(F.col("payment_type") == 6, "Voided Trip")
                .otherwise("Unknown")
            )

            # decode do ratecodeid
            .withColumn(
                "ratecode_desc",
                F.when(F.col("ratecodeid") == 1, "Standard rate")
                .when(F.col("ratecodeid") == 2, "JFK")
                .when(F.col("ratecodeid") == 3, "Newark")
                .when(F.col("ratecodeid") == 4, "Nassau or Westchester")
                .when(F.col("ratecodeid") == 5, "Negotiated fare")
                .when(F.col("ratecodeid") == 6, "Group ride")
                .otherwise("Unknown")
            )

            # aditoria silver
            .withColumn("_silver_timestamp", F.current_timestamp())
            .withColumn("_silver_pipeline",  F.lit("yellow_tripdata_silver"))
        )

        # Seleção final — descarta colunas operacionais da bronz
        AUDIT_BRONZE_COLS = [
            "_src_file_path", "_src_file_name", "_src_file_mod_time",
            "_ingestion_timestamp", "_src_row_hash", "_pipeline_name",
            "_environment", "_load_mode", "load_ts"
        ]

        return df_enriched.drop(*AUDIT_BRONZE_COLS)


    @property
    def partitions(self):
        return ["pickup_year", "pickup_month", "vendorid"]


    @property
    def merge_key(self):
        return ["vendorid", "tpep_pickup_datetime", "pulocationid", "dolocationid"]

# COMMAND ----------

full_load = Widget.get_or_else("full_load", True)
YellowTripData(full_load=full_load).run(operation="overwrite")
