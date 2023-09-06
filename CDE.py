# Databricks notebook source
pip install openpyxl

# COMMAND ----------

# MAGIC %md
# MAGIC #prepare df

# COMMAND ----------

# MAGIC %md
# MAGIC ##import lib

# COMMAND ----------

import pyspark.pandas as ps
from pyspark.sql.functions import col, when, count, lit
from pyspark.sql import DataFrame
from datetime import datetime
from pyspark.sql.types import DateType

# COMMAND ----------

# MAGIC %md
# MAGIC ##all_granular

# COMMAND ----------

path_rule = "dbfs:/mnt/dmbd-dg/Dq_data_audit_dashboard/Production/external_dq_rules_and_conditions.xlsx"

def load_all_sheet_xlsx(path_rule:str, ):
    daily = ps.read_excel(io = path_rule, engine='openpyxl', sheet_name = 'Daily')
    daily = daily.to_spark()

    weekly = ps.read_excel(io = path_rule, engine='openpyxl', sheet_name = 'Weekly')
    weekly = weekly.to_spark()

    monthly = ps.read_excel(io = path_rule, engine='openpyxl', sheet_name = 'Monthly')
    monthly = monthly.to_spark()
    return daily,weekly,monthly

# COMMAND ----------

# MAGIC %md
# MAGIC ##rules

# COMMAND ----------

def rule_clean(dq_rules_spark:DataFrame):
    rules_cleaned_df = dq_rules_spark.withColumn("Logic_SQL", when(col("Logic_spark").isNull(), col("Logic_SQL"))
                                                .otherwise(None))\
                                                .filter(col("Logic_spark").isNotNull() | col("Logic_SQL").isNotNull())
    return rules_cleaned_df

# COMMAND ----------

# MAGIC %md
# MAGIC #main

# COMMAND ----------

daily,weekly,monthly = load_all_sheet_xlsx(path_rule)

daily = rule_clean(daily)
weekly = rule_clean(weekly)
monthly = rule_clean(monthly)

daily = daily.select('Table_name', 'Column')
weekly = weekly.select('Table_name', 'Column')
monthly = monthly.select('Table_name', 'Column')

concatenated_df = daily.union(weekly).union(monthly)
grouped_df = concatenated_df.groupBy('Table_name', 'Column')
CDE_c360_external = grouped_df.agg(count('*').alias('Count_rules_on_col'))

exec_date = datetime.today().strftime("%Y-%m-%d")
CDE_c360_external = CDE_c360_external.withColumn("Exec_date", lit(exec_date))
CDE_c360_external = CDE_c360_external.withColumn("Exec_date", col("Exec_date").cast(DateType()))

CDE_c360_external.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #save result

# COMMAND ----------

# MAGIC %md
# MAGIC ##to db

# COMMAND ----------

def save_df_to_databricks(granular:str, istest:str = "test_"):
    CDE_c360_external.write.format("delta").mode("append").saveAsTable(f"dg_dq_report.{istest}c360_external_dq_rule_{granular}")

save_df_to_databricks(granular = 'cde', istest = '')

# COMMAND ----------

def show_df_from_db_cde(granular:str, istest:str = "test_"):
    cde = spark.sql(f"SELECT * FROM dg_dq_report.{istest}c360_external_dq_rule_{granular}")
    return cde

cde = show_df_from_db_cde(granular = 'cde', istest = '')
cde = cde.filter(cde.Exec_date == "2023-06-27")
cde.display()

# COMMAND ----------

from pyspark.sql.functions import sum, max, col

# Assuming you have a DataFrame called 'cde' with columns 'Exec_date' and 'Count_rules_on_col'
# Filter the DataFrame to get the maximum date
max_date = cde.select(max(col('Exec_date'))).collect()

# COMMAND ----------

# Assuming you have a DataFrame called 'cde' with columns 'Exec_date' and 'Count_rules_on_col'
# Filter the DataFrame to get the maximum date
max_date = cde.select(max(col('Exec_date'))).collect()[0][0]

# Filter the DataFrame based on the maximum date
filtered_cde = cde.filter(cde.Exec_date == max_date)

# Calculate the sum of 'Count_rules_on_col' in the filtered DataFrame
total_sum = filtered_cde.select(sum(filtered_cde.Count_rules_on_col)).collect()[0][0]

# Print the total sum
print(total_sum)

# COMMAND ----------


