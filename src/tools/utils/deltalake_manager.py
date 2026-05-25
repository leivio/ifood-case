# Databricks notebook source
# MAGIC %run ./utils

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *
from pyspark.sql import functions as F

class DeltalakeManager:

    def __init__(self, entity, database='nyc_tlc', route_path="nyc_tlc", merge_key=None, partitions=None, debug=False, layer="", separate_underscore = 2, catalog='frt_am'):
        self.layer = layer
        self.separate_underscore = '_' * separate_underscore
        self.route_path = route_path
        self.entity = entity.lower()
        self.database = database.lower()
        self.region = "am"
        self.env = dbutils.secrets.get('nyc_tlc', 'env').lower()
        self.catalog = catalog
        self.uc_catalog = self.get_uc_catalog()
        self.uc_schema = self.layer
        self.uc_table = f"{self.uc_catalog}.{self.uc_schema}.{self.database}{self.separate_underscore}{self.entity}"
        self.merge_key = self.__get_merge_key(merge_key)
        self.partitions = self.__get_partitions(partitions)
        self.default_replace = ""
        self.debug = debug
        self.debug_show()
        self.create_schema(self.uc_catalog, self.uc_schema)


    @property
    def is_delta_table(self):       

        spark.catalog.setCurrentCatalog(self.uc_catalog)
        spark.catalog.setCurrentDatabase(self.uc_schema)
        tabelas = spark.catalog.listTables()
        tabela_nomes = [tabela.name for tabela in tabelas]
        return f"{self.database}{self.separate_underscore}{self.entity}" in tabela_nomes


    def create_schema(self, catalog, schema):
        if catalog is None:
            raise Exception("catalog inválido ou não informado.")

        if not schema or not schema.strip():
            raise Exception("Schema inválido ou não informado.")

        spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
        schemas = [row[0] for row in spark.sql(f"SHOW SCHEMAS IN {catalog}").collect()]

        if schema not in schemas:
            spark.sql(f"CREATE SCHEMA {catalog}.{schema}")


    def print_msg(self, msg="", print_dotted=True):
        if self.debug:
            if print_dotted:
                Utils.print_dotted_lines(msg)
            else:
                print(msg)


    def debug_show(self):
        sufix_catalog = {
            "sit": "Sit",
            "uat": "Uat",
            "prd": "Prd"}

        sufix_region = {
            "am": "Americas",
            "eu": "Europa"}

        if self.debug:
              infor = f"catalog={self.catalog} database={self.database} entity={ self.entity} region={sufix_region[self.region]} environment={sufix_catalog[self.env]}"
              self.print_msg(infor, False)
              Utils.print_dotted_lines()


    def get_region_env(self):
        sufix_catalog = {
            "sit": "_sit",
            "uat": "_uat",
            "prd": ""}
        return f"{self.region}{sufix_catalog[self.env]}"


    def get_uc_catalog(self):
        return f"{self.catalog}_{self.get_region_env()}"


    def __get_merge_key(self, merge_key):
        if isinstance(merge_key, list):
            ss = ""
            for key in merge_key:
                ss = ss + f"target.{key} = updates.{key} and "
            return ss[0:len(ss) - 4]

    def __check_merge_key(self):
        if self.merge_key is None:
            raise Exception("Merge key not found.")


    def __get_partitions(self, partitions):
        if isinstance(partitions, str):
            return partitions.split(",")
        elif isinstance(partitions, list):
            return partitions


    def entity_path(self):
        location = spark.sql(f"DESCRIBE DETAIL {self.uc_table}").select("location").collect()[0][0]
        return location


    def load(self):                
        return spark.table(self.uc_table)
    

    def __set_cols_expr(self, cols):
        if isinstance(cols, (list, tuple)):
            for c in cols:
                cols = dict(zip(cols, ["{}{}".format("updates.", c) for c in cols]))
        return cols


    def __set_source_df(self, dataframe):
        target_cols = set(self.load().columns)
        source_cols = set(dataframe.columns)
        return dataframe.select(list(set.intersection(target_cols, source_cols)))


    def __add_default_columns(self, df):
        if self.uc_schema.startswith("bronze"):
            load_name = "load_ts"
            df = df.withColumn(load_name, F.current_timestamp())
        return df


    def __update_metadata(self):
        sql_metadata = f"MSCK REPAIR TABLE {self.uc_table} SYNC METADATA;"
        spark.sql(sql_metadata)


    def __delta_writer(self, dataframe):
        return dataframe \
            .transform(self.__add_default_columns) \
            .write \
            .format("delta")


    def __delta_table(self):
        return DeltaTable.forName(spark, self.uc_table)


    def save(self, dataframe):
        self.print_msg("DeltalakeManager.save")
        #spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")

        self.__delta_writer(dataframe) \
            .clusterByAuto() \
            .saveAsTable(self.uc_table)
        self.__update_metadata()
        self.print_msg("done")


    def save_partition(self, dataframe):
        self.print_msg("DeltalakeManager.save_partition")

        self.__delta_writer(dataframe) \
            .clusterBy(self.partitions) \
            .saveAsTable(self.uc_table)
        self.__update_metadata()
        self.print_msg("done")


    def overwrite(self, dataframe):
        self.print_msg("DeltalakeManager.overwrite")

        self.__delta_writer(dataframe) \
        .mode("overwrite") \
        .clusterBy(self.partitions) \
        .option("overwriteSchema", "true") \
        .saveAsTable(self.uc_table)

        self.__update_metadata()
        self.print_msg(f"{self.uc_table} saved in full overwrite mode.", False)
        self.print_msg(f"Location: {self.entity_path()}", False)
        self.print_msg("done")


    def overwrite_partition(self, dataframe):
        self.print_msg("DeltalakeManager.overwrite_partition")

        self.__delta_writer(dataframe) \
            .option("mergeSchema", "true") \
            .mode("overwrite") \
            .clusterBy(self.partitions) \
            .saveAsTable(self.uc_table)

        self.__update_metadata()
        self.print_msg(f"{self.uc_table} saved by partitions in overwrite mode.", False)
        self.print_msg(f"Partitions: {self.partitions}", False)
        self.print_msg(f"Location: {self.entity_path()}", False)
        self.print_msg("done")


    def overwrite_force_partition(self, dataframe):
        self.print_msg("DeltalakeManager.overwrite_force_partition")
        #spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")

        self.__delta_writer(dataframe) \
            .option("overwriteSchema", "true") \
            .mode("overwrite") \
            .clusterBy(self.partitions) \
            .saveAsTable(self.uc_table)

        self.__update_metadata()
        self.print_msg(f"{self.uc_table} saved by partitions in overwrite mode.", False)
        self.print_msg(f"Partitions: {self.partitions}", False)
        self.print_msg(f"Location: {self.entity_path()}", False)
        self.print_msg("done")


    def append(self, dataframe):
        self.print_msg("DeltalakeManager.append")

        self.__delta_writer(dataframe) \
            .option("mergeSchema", "true") \
            .mode("append") \
            .saveAsTable(self.uc_table)

        self.__update_metadata()
        self.print_msg("done")


    def append_partition(self, dataframe):
        self.print_msg("DeltalakeManager.append_partition")

        self.__delta_writer(dataframe) \
            .option("mergeSchema", "true") \
            .mode("append") \
            .clusterBy(self.partitions) \
            .saveAsTable(self.uc_table)

        self.__update_metadata()
        self.print_msg("done")


    def replace_partition(self, dataframe, partition_name, partition_value):

        if self.is_delta_table:
            self.print_msg("DeltalakeManager.replace_partition")
            replace_statement = f"""{self.default_replace} and {partition_name} = '{partition_value}'"""
            dataframe \
                .transform(self.__add_default_columns) \
                .write \
                .format("delta") \
                .mode("overwrite") \
                .option("mergeSchema", "true") \
                .option("replaceWhere", replace_statement) \
                .option("path", self.entity_path()) \
                .saveAsTable(self.uc_table)
            self.__update_metadata()
            self.print_msg("done")
        else:
            self.save_partition(dataframe)


    def merge(self, dataframe, cols_to_update=None, ignore_target_columns=False):
        self.__check_merge_key()

        if self.is_delta_table:
            self.print_msg("DeltalakeManager.merge")
            dataframe = dataframe.transform(self.__add_default_columns)

            if (cols_to_update is not None) & (ignore_target_columns):
                raise Exception("Merge not accepted with two parameters filled.")

            if not ignore_target_columns:
                self.print_msg(f"ignore_target_columns {ignore_target_columns}", False)
                dataframe = self.__set_source_df(dataframe)

            if cols_to_update:
                self.print_msg(f"cols_to_update {cols_to_update}", False)
                #spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", False)
                self.__delta_table() \
                    .alias("target") \
                    .merge(dataframe.alias("updates"), self.merge_key) \
                    .whenMatchedUpdate(set = self.__set_cols_expr(cols_to_update)) \
                    .whenNotMatchedInsert(values = self.__set_cols_expr(cols_to_update)) \
                    .execute()

                self.print_msg()
            else:
                #spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", True)
                self.__delta_table() \
                    .alias("target") \
                    .merge(dataframe.alias("updates"), self.merge_key) \
                    .whenMatchedUpdateAll() \
                    .whenNotMatchedInsertAll() \
                    .execute()

                self.print_msg("done")
        else:
            self.save_partition(dataframe)
        self.__update_metadata()
