# Databricks notebook source
pip install openpyxl

# COMMAND ----------

# MAGIC %md
# MAGIC #Import data

# COMMAND ----------

# MAGIC %md
# MAGIC ##Rule

# COMMAND ----------

import pyspark.pandas as ps
import pandas as pd

path_rule = "dbfs:/mnt/dmbd-dg/Dq_data_audit_dashboard/Production/external_dq_rules_and_conditions.xlsx"
dq_rules_ps = ps.read_excel(io = path_rule, engine='openpyxl', sheet_name = 'Monthly')
dq_rules_spark = dq_rules_ps.to_spark()
dq_rules_spark.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Condition

# COMMAND ----------

path_rule = "dbfs:/mnt/dmbd-dg/DQ_rule/external_table/external_dq_rule&Conditions.xlsx"
dq_conditions_ps = ps.read_excel(io = path_rule, engine='openpyxl', sheet_name = 'Condition')
dq_conditions_spark = dq_conditions_ps.to_spark()
dq_conditions_spark.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Partition

# COMMAND ----------

all_table_partitions = spark.sql("select * from dg_dq_report.data_quality_report")
all_table_partitions.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Main

# COMMAND ----------

# MAGIC %md
# MAGIC ##Function

# COMMAND ----------

# Define the UDF for generate_dates
def generate_dates(start_date, end_date, excluded_dates):
    dates = []
    current_date = start_date

    while current_date <= end_date:
        if current_date not in excluded_dates:
            dates.append(current_date)
        current_date += relativedelta(months=1)
    return dates

# COMMAND ----------

# MAGIC %md
# MAGIC ###lib

# COMMAND ----------

from pyspark.sql.functions import col, when, lit, max, expr, to_timestamp, date_format, isnan, collect_list, array, first, map_concat, to_json, from_json, map_filter, format_number, round, concat
from pyspark.sql.types import ArrayType, DateType, MapType, StructField, StringType, IntegerType, StructType, DoubleType, LongType
from pyspark.sql import DataFrame
from datetime import datetime, date,timedelta
from dateutil.relativedelta import relativedelta

# COMMAND ----------

table_success_db = "dg_dq_report.c360_external_dq_rule_success_monthly"
partition_success_db = "dg_dq_report.c360_external_dq_rule_success_monthly_partitions"
table_fail_db = "dg_dq_report.c360_external_dq_rule_fail_monthly"
partition_fail_db = "dg_dq_report.c360_external_dq_rule_fail_monthly_partitions"

# COMMAND ----------

# MAGIC %run "/Users/chanwitt@ais.co.th/DQ_Audit_Dashboard_Report/incremental/Main"

# COMMAND ----------

# MAGIC %md
# MAGIC #save result

# COMMAND ----------

# MAGIC %md
# MAGIC ##to db

# COMMAND ----------

good_result_output.display()
history_good_result_output.display()
error_result_output.display()
history_error_result_output.display()

# COMMAND ----------

save_df_to_databricks_by_granular(table_success_db, partition_success_db, table_fail_db, partition_fail_db)

# COMMAND ----------


