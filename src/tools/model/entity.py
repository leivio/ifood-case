# Databricks notebook source
# MAGIC %run ./../utils/deltalake_manager

# COMMAND ----------

import uuid
import traceback
import decimal
from abc import ABC, abstractmethod
from pyspark.sql.types import *
from datetime import date, datetime, timedelta

class Entity(ABC):

    def __init__(self, catalog, debug=False):
        self.set_debug(debug)
        self.__ldlm = DeltalakeManager("nyc_tlc_log", "track", "log", layer="track", catalog=self.catalog)
        self.init_duration=-1
        self.uuid = str(uuid.uuid4())


    def get_debug(self):
        return self.__debug


    def set_debug(self, var):
        self.__debug = var

    debug = property(get_debug, set_debug)


    @property
    @abstractmethod
    def database(self):
        pass


    @property
    @abstractmethod
    def entity(self):
        pass


    @property
    def define_tack_table(self):
        return "nyc_tlc_log"


    def init_run(self):
        self.init_duration = datetime.now()


    def fim_run(self):
        if self.init_duration == -1:
            return 0
        else:
            return (datetime.now() - self.init_duration).total_seconds()


    def set_log(self, df_count, refDate, msg, e=None, tag=None):
        date_ts = datetime.now()
        stack = ""
        full_class_name = ""
        if e:
            full_class_name = self.__get_full_class_name(e)
            stack =  ''.join(traceback.format_exception(e))

        data = [
                (self.uuid, date_ts, self.database,
                 self.entity, msg, self.fim_run(),
                 full_class_name, stack, df_count, refDate, tag)
               ]

        schema = StructType([
            StructField("id", StringType(),True),
            StructField("date", TimestampType(),True),
            StructField("database", StringType(),True),
            StructField("entity", StringType(),True),
            StructField("message", StringType(),True),
            StructField("duration", FloatType(),True),
            StructField("type",StringType(),True),
            StructField("args",StringType(),True),
            StructField("count",LongType(),True),
            StructField("refDate", StringType(), True),
            StructField("tag", StringType(), True)
           ])

        df = spark.createDataFrame(data=data, schema=schema)
        self.__ldlm.append(df)

        if self.__debug:
            if e:
                raise e
        else:
            print(stack)            
            if msg=="FAILED":
                pass
                # TeamsNotifier().sent()


    def __get_full_class_name(self, obj):
            module = obj.__class__.__module__
            if module is None or module == str.__class__.__module__:
                return obj.__class__.__name__
            return module + '.' + obj.__class__.__name__


    def log(self, id=None):
        if id:
            return self.__ldlm.load().filter(col("id") == id)
        else:
            if id==0:
                return self.__ldlm.load().filter(col("id") == self.uuid)
            else:
                return self.__ldlm.load() \
                    .filter(col("database") == self.database) \
                    .filter(col("entity") == self.entity)
