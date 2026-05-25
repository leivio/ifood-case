# Databricks notebook source
# MAGIC %run ./../../tools/model/extract

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, TimestampType
from pyspark.sql import DataFrame
from functools import reduce

class YellowTripData(Extract):


    # Schema canônico alvo (tudo lowercase)
    @property
    def CANONICAL_SCHEMA(self):
        return StructType([
                StructField("vendorid",              LongType(),         True),
                StructField("tpep_pickup_datetime",  TimestampNTZType(), True),
                StructField("tpep_dropoff_datetime", TimestampNTZType(), True),
                StructField("passenger_count",       LongType(),         True),
                StructField("trip_distance",         DoubleType(),       True),
                StructField("ratecodeid",            LongType(),         True),
                StructField("store_and_fwd_flag",    StringType(),       True),
                StructField("pulocationid",          LongType(),         True),
                StructField("dolocationid",          LongType(),         True),
                StructField("payment_type",          LongType(),         True),
                StructField("fare_amount",           DoubleType(),       True),
                StructField("extra",                 DoubleType(),       True),
                StructField("mta_tax",               DoubleType(),       True),
                StructField("tip_amount",            DoubleType(),       True),
                StructField("tolls_amount",          DoubleType(),       True),
                StructField("improvement_surcharge", DoubleType(),       True),
                StructField("total_amount",          DoubleType(),       True),
                StructField("congestion_surcharge",  DoubleType(),       True),
                StructField("airport_fee",           DoubleType(),       True),
            ])    


    @property
    def database(self):
        return "nyc_tlc"


    @property
    def layer(self):
        return "bronze"    
    

    @property
    def entity(self):
        return "YellowTripdata"
    

    # Listagem de arquivos com filtro por data de modificação
    def list_parquet_files(self, base_path):
        cutoff_ms = None
        if self.full_load == False:
            cutoff_dt = datetime.utcnow() - timedelta(days=(self.offset_days * -1))
            cutoff_ms = int(cutoff_dt.timestamp() * 1000)  # modificationTime é em ms
            print(f"Cutoff: {cutoff_dt.strftime('%Y-%m-%d %H:%M:%S')} UTC")

        result = []

        def _recurse(path):
            for item in dbutils.fs.ls(path):
                if item.path.endswith("/"):
                    _recurse(item.path)
                elif item.path.endswith(".parquet"):
                    if cutoff_ms is None or item.modificationTime >= cutoff_ms:
                        result.append(item.path)

        _recurse(base_path)
        return result
        
    # Leitura individual + normalização
    def read_and_normalize(self, path: str):
        df = spark.read.parquet(path)

        # lowercase em todos os nomes
        df = df.toDF(*[c.lower() for c in df.columns])

        CANONICAL_MAP  = {f.name: f.dataType for f in self.CANONICAL_SCHEMA.fields}
        self.CANONICAL_COLS = list(CANONICAL_MAP.keys())

        # cast para tipo canônico + preenche ausentes com null
        for col_name, dtype in CANONICAL_MAP.items():
            if col_name in df.columns:
                df = df.withColumn(col_name, F.col(col_name).cast(dtype))
            else:
                df = df.withColumn(col_name, F.lit(None).cast(dtype))

        # metadados do arquivo
        df = (
            df
            .withColumn("_src_file_path",     F.col("_metadata.file_path"))
            .withColumn("_src_file_name",     F.col("_metadata.file_name"))
            .withColumn("_src_file_mod_time", F.col("_metadata.file_modification_time"))
        )    
            
        return df.select(self.CANONICAL_COLS + ["_src_file_path","_src_file_name","_src_file_mod_time"])    
    

    @property
    def transformer(self):

        LOAD_MODE = "FULL" if self.full_load == True else "INCREMENTAL"

        print(f"full mode: {self.full_load} | offset days: { self.offset_days }")

        parquet_files = self.list_parquet_files( "/Volumes/frt_am/landing/data_prd/yellow_tripdata/")
        print(f"Arquivos encontrados: {len(parquet_files)}")

        if not parquet_files:
            print("Nenhum arquivo encontrado para o período. Encerrando.")
            dbutils.notebook.exit("NO_FILES")

        dfs = [self.read_and_normalize(f) for f in parquet_files]

        df_union = Utils.union_all(dfs)

        # #df_union = reduce(DataFrame.unionByName, dfs) - Limitacao DBX FREE -> Directly accessing the underlying Spark driver JVM using the attribute '_jdf' is not supported on serverless compute.

        pipeline_name = "yellow_trip_data"
        _maps  = {f.name: f.dataType for f in self.CANONICAL_SCHEMA.fields}
        _cols = list(_maps.keys())

        df =  (
            df_union
            .withColumn("_ingestion_timestamp", F.current_timestamp())
            .withColumn("_src_row_hash",        F.md5(F.concat_ws("|", *_cols)))
            .withColumn("_pipeline_name",       F.lit(pipeline_name))
            .withColumn("_environment",         F.lit("prd"))
            .withColumn("_load_mode",           F.lit(LOAD_MODE))
        )

        return df


    @property
    def partitions(self):
        return ["VendorID", "tpep_pickup_datetime"]


    @property
    def merge_key(self):
        return ["VendorID", "tpep_pickup_datetime"]

# COMMAND ----------

full_load = Widget.get_or_else("full_load", True)
YellowTripData(full_load=full_load).run(operation="overwrite")
