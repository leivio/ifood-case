# Databricks notebook source
# MAGIC %run ./entity

# COMMAND ----------

# MAGIC %run ../utils/utils

# COMMAND ----------

# MAGIC %run ../utils/widget

# COMMAND ----------

# MAGIC %run ../utils/deltalake_manager

# COMMAND ----------


import traceback
from pyspark.sql import DataFrame
from abc import ABC, abstractmethod
from delta.tables import *
from pyspark.sql import functions as F
from datetime import date, timedelta, datetime
from pyspark.sql.functions import to_date

class Extract(Entity):

    def __init__(self, full_load=False, offset_days=-1, debug=False):
        self.debug = debug        
        self.refDate = str(date.today())
        self.offset_days = offset_days
        self.full_load = False if full_load is None else full_load
        super().__init__(self.debug)
        self.target_date = Utils.get_date_by_offset(self.offset_days)
        self.dlm = DeltalakeManager(self.entity, self.database, "nyc_tlc", self.merge_key, self.partitions, debug, self.layer, self.separate_underscore, catalog=self.catalog)
        self.optimize_write = True
        self.auto_compact = True        
     

    @property
    def separate_underscore(self):
        return 2


    @property
    def define_tack_table(self):
        return "extract"


    @property
    def catalog(self):
        return 'frt'


    @property
    def expectations(self):
        return Optional[None]
    

    @property
    @abstractmethod
    def database(self):
        return None


    @property
    @abstractmethod
    def merge_key(self):
        return None


    @property
    @abstractmethod
    def partitions(self):
        return None


    @property
    def get_df(self) :
        return self.transformer


    @property
    @abstractmethod
    def transformer(self):
        return None

    
    @property
    def get_enabled_environment(self):
        return ["prd", "uat"] 
    

    @property
    def load(self):
        return self.dlm.load()
    

    @property
    def source_table(self):
        return None    

    def save(self, operation=None):

        print(f"Full load: {self.full_load}")
               
        dataframe = self.get_df
        if dataframe:
            self.count = dataframe.count()
            if operation is None:
                operation = "merge"

            print(f"operation {operation}")

            tag = 'full'
            if operation=='merge':
                tag = 'merge'

            if operation=='merge':
                self.dlm.merge(dataframe)
            elif operation=='overwrite':
                if self.partitions is None or len(self.partitions) == 0:
                    self.dlm.overwrite(dataframe)
                else:
                    self.dlm.overwrite_force_partition(dataframe)
            elif operation=='overwrite_force':
                self.dlm.overwrite_force_partition(dataframe)
            elif operation=='overwrite_partition':
                self.dlm.overwrite_partition(dataframe)
            else:
                self.dlm.overwrite_partition(dataframe)

            self.set_log(self.count, self.refDate, "OK", tag=tag)
            print(f"SUCCESS  {self.entity} ({self.count}) - [{self.uuid}]")
        else:
            print(f"dataframe is none {self.entity} [{self.uuid}]")


    def run(self, operation=None):
        if self.dlm.env in self.get_enabled_environment:            
            try:                
                self.init_run()                                
                self.save(operation)                       
            except Exception as e:
                final_msg = f"FAILED {self.entity} ({self.count})- [{self.uuid}]"            
                self.set_log(self.count, self.refDate, "FAILED", e)
        else:
            print(f"environment not enabled {self.dlm.env} {self.entity} [{self.uuid}]") 
