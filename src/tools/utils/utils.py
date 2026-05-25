# Databricks notebook source
from pyspark.sql.functions import lit, col, trim, size, expr, explode, struct, collect_list, lower
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from dateutil.relativedelta import relativedelta
from datetime import date, timedelta, datetime
from pyspark.sql import DataFrame
from calendar import monthrange
from functools import reduce
from delta.tables import *
import unicodedata
import base64
import uuid
import re
import os
import traceback


class Utils:

    @staticmethod
    def get_full_class_name(obj):
        module = obj.__class__.__module__
        if module is None or module == str.__class__.__module__:
            return obj.__class__.__name__
        return module + '.' + obj.__class__.__name__


    @staticmethod
    def get_region(country):
        return "am"
   

    @staticmethod
    def upper_camel_case_remove_underscore(str_value):
        r = ""
        b = False
        loop = 0
        for s in str_value:
            if loop == 0:
                r = r + s.upper()
                loop = 1
            elif (s == "-") or (s == "_"):
                b = True
            else:
                if b:
                    r = r + s.upper()
                    b = False
                else:
                    r = r + s.lower()
        return r


    @staticmethod
    def print_dotted_lines(value="", level=0):
        char = " "
        char2= "-"
        if value == "":
            print(char2 * 66)
        else:
            tam = len(value) // 2
            exibit = f"{char * level}{char2 * (32 - tam)} {value} {char2 * (32 - tam)}"
            if (len(exibit) % 2) != 0:
                print(exibit[:-1])
            else:
                print(exibit)


    @staticmethod
    def check_if_column_exists(df, col_name):
        return col_name in [column.lower() for column in df.columns]


    @staticmethod
    def get_date_by_offset(offset:int=0):
        return F.date_add(F.current_date(), int(offset))


    @staticmethod
    def get_str_date_by_offset_days(offset):
        """
        Return a date in str format considering the defined offset in days.
        """
        currentDatetime = datetime.now()
        targetDatetime = currentDatetime - timedelta(days=offset)
        return targetDatetime.strftime('%Y-%m-%d')


    @staticmethod
    def get_str_date_by_offset_months(offset=-1, first_day=True):
        """
        Return the first month day in str format considering the defined offset in months.
        """
        patern = '%Y-%m-01' if first_day else '%Y-%m-%d'
        current_date_time = datetime.now()
        target_date_time = current_date_time + relativedelta(months=offset)
        return target_date_time.strftime(patern)


    @staticmethod
    def upper_col_names(df):
        """
        Turn all dataframe columns uppercased.
        """
        return df.select([col(x).alias(x.upper()) for x in df.columns])


    @staticmethod
    def lower_col_names(df):
        """
        Turn all dataframe columns lowercased.
        """
        return df.select([col(x).alias(x.lower()) for x in df.columns])


    @staticmethod
    def trim_all_str_cols(df):
        """
        Trim all column values from dataframe
        """
        for column in df.columns:
            if df.schema[column].dataType == StringType():
                df = df.withColumn(column, trim(col(column)))
        return df


    @staticmethod
    def normalize_text(text):
        """
        Remove non ascii chars from text.
        """
        if text:
            return unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode()
        else:
            return text


    @staticmethod
    def add_prefix_to_cols(df, cols, prefix):
        """
        Rename column names with a defined prefix.
        """
        for col in cols:
            df = df.withColumnRenamed(col, "".join([prefix, "_", col]))
        return df


    @staticmethod
    def get_effectiveness_date_range(days_offset=3, offset_date=date.today()):
        """
        Return a valid date range based on business rules of project
        to be used to filter datasets in effectiveness processes.
        """
        days_in_month = lambda dt: monthrange(dt.year, dt.month)[1]
        day = int(offset_date.strftime("%d"))
        if (day <= days_offset):
            today = offset_date - timedelta(days=day)
        else:
            today = offset_date
        first_day = today.replace(day=1)
        if days_offset == 0:
            last_day = today.replace(day=monthrange(today.year, today.month)[1])
        else:
            last_day = today.replace(day=days_offset) + timedelta(days_in_month(today))
        return first_day, last_day


    @staticmethod
    def load_delta_table(path):
        """
        Load delta tables from path.
        """
        return spark.read.format("delta").load(path)


    @staticmethod
    def dump_sub_json_item(payload, item_name):
        """
        Dump json field created by spark inside a json document.
        """
        parsed_payload = []
        try:
            for item in payload:
                if item[item_name]:
                    item[item_name] = json.loads(item[item_name])
                else:
                    del item[item_name]
                parsed_payload.append(item)
        except ValueError:
            print ('ValueError: could not parse invalid payload')
        return payload


    @staticmethod
    def get_max_col_value(dataframe, col_name):
        """
        Get max value from a column in dataframe.
        """
        return dataframe.select(col_name).rdd.max()[0]


    @staticmethod
    def get_min_col_value(dataframe, col_name):
        """
        Get min value from a column in dataframe.
        """
        return dataframe.select(col_name).rdd.min()[0]


    @staticmethod
    def regexp_extract_all(text, pattern):
        """
        Get all matches of a regex pattern from a string
        """
        text = "" if text is None else text
        pattern = re.compile(pattern, re.M)
        return re.findall(pattern, text)


    @staticmethod
    def check_delta_table(table_path):
        """
        Check if a delta table exists
        """
        return True if DeltaTable.isDeltaTable(spark, table_path) else False


    @staticmethod
    def check_sql_table(db, table):
        """
        Check if a delta SQL table exists in a database
        """
        tabelas = spark.catalog.listTables(db)
        tabela_nomes = [tabela.name for tabela in tabelas]
        return table in tabela_nomes


    @staticmethod
    def collect_col_values(dataframe, col_name, ignore_list="-"):
        """
        Collect distinct values from a dataframe col to a list
        """
        if dataframe:
            values = dataframe \
                .select(col_name) \
                .distinct() \
                .filter(~col(col_name).isin(ignore_list))
            result = [col[col_name] for col in values.collect()]
            result.sort()
            return result
        else:
            return dataframe


    @staticmethod
    def is_df(obj):
        """
        Check if an object is a Spark DataFrame
        """
        return True if isinstance(obj, DataFrame) else False


    @staticmethod
    def union_all_df(dataframes_list, distinct=False):
        """
        Concatenate dataframes inside dataframes_list params into a unique df.
        """
        try:
            df = reduce(DataFrame.unionByName, dataframes_list)
            if distinct:
                df = df.distinct()
            return df
        except Exception as e:
            print("DataFrame UnionByName function failed.")
            print(e)


    @staticmethod
    def union_all(dfs: list):
        """
        cumula via loop simples — evita encadeamento profundo do reduce.
        """
        result = dfs[0]
        for df in dfs[1:]:
            result = result.unionByName(df, allowMissingColumns=True)
        return result            


    @staticmethod
    def create_empty_df(schema):
        """
        Create an empty dataframe based on a given schema.
        """
        return spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)



    @staticmethod
    def remove_duplicated_rows(dataframe, *primary_key_cols):
        """
        Remove duplicated rows found by given key cols.
        """
        dataframe.cache()
        duplicates = dataframe \
            .groupBy(list(primary_key_cols)) \
            .count() \
            .filter("count > 1")

        print(f"{duplicates.count()} duplicated rows found.")
        return dataframe.join(duplicates, list(primary_key_cols), "leftanti")


    @staticmethod
    def read_file(format, path, **params):
        """
        Read spark supported files.
        """
        return spark.read.format(format).options(**params).load(path)


    @staticmethod
    def write_file(df, format, mode, path, **params):
        """
        Write spark supported files.
        """
        df.write.format(format).options(**params).mode(mode).save(path)
        print(f"writed into: {path}")


    @staticmethod
    def get_dates_path(date=datetime.now()):
        """
        Make dates string path formated to beesforce delta lake standart
        """
        return os.path.join(
            date.strftime("%Y"),
            date.strftime("%Y%m"),
            date.strftime("%Y%m%d"))


    @staticmethod
    def enforce_column(dataframe, col_name, col_type="string", col_value=None):
        """
        Creates a column in dataframe if the col not exists.
        """
        if col_name not in dataframe.columns:
            dataframe = dataframe \
                .withColumn(col_name, lit(col_value).cast(col_type))
        return dataframe


    @staticmethod
    def count_list_difference(ref_list, target_list):
        """
        Return the uantity of different items between two lists.
        """
        set_ref = set(ref_list)
        set_target = set([] if target_list == None else target_list)
        return len(set_ref.difference(set_target))


    @staticmethod
    def convert_to_uuid(data):
        res_uuid = None
        if data is not None:
            message_bytes = base64.b64decode(data)
            res_uuid = str(uuid.UUID(bytes=message_bytes))
        return res_uuid



    @staticmethod
    def get_list_to_str(list):
        return ",".join([f"'{element}'" for element in list])


    @staticmethod
    def run_no_stop(
        notebook,
        args,
        no_stop=True,
        job_logger=None,
        send_alert=False,
        alert_metadata={},
        job_context=None
    ):
        try:
            return dbutils.notebook.run(notebook, 3600, args)
        except Exception as e:
            logger = job_logger if job_logger else CustomLogger(context="Run No Stop")
            retry_message = "Retrying error" if no_stop else ""
            logger.log_error(
                message=e,
                slack_alert=send_alert,
                alert_metadata=alert_metadata,
                job_context=job_context
            )

            if not no_stop:
                raise e


    @staticmethod
    def guid_to_uuid(value):
        if value is None:
            return None
        else:
            _g0 = value.replace("-", "")
            _g1 = re.sub(r"(.{8})(.{4})(.{4})(.{4})(.{12})", r"\1-\2-\3-\4-\5", _g0)
            _g2 = re.sub(r"(.{2})(.{2})(.{2})(.{2})(.{3})(.{2})(.{3})(.{2})(.{18})", r"\4\3\2\1-\6\5-\8\7-\9", _g1)
            _g3 = _g2.replace("-", "")
            return re.sub(r"(.{8})(.{4})(.{4})(.{4})(.{12})", r"\1-\2-\3-\4-\5", _g3)


    @staticmethod
    def get_str_date_by_offset_hours(offset):
        """
        Return a date in str format considering the defined offset in minutes.
        """
        currentDatetime = datetime.now()
        targetDatetime = currentDatetime - timedelta(minutes=offset)
        return targetDatetime


    @staticmethod
    def date_start_of_day(date_time: datetime) -> datetime:
        """
        Truncate a datetime to the start of the day.

        Args:
            date_time (datetime): The datetime variable to be truncated.

            Returns:
            datetime: A date truncated to the start of the day.
        """
        return date_time.replace(hour=0, minute=0, second=0, microsecond=0)
