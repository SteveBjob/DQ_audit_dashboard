# Databricks notebook source
# Define the UDF (User-Defined Function) for parsing and converting excluded_dates
def parse_excluded_dates(excluded_dates):
    excluded_dates = excluded_dates.strip("[]").split(" | ")
    excluded_dates = [datetime.strptime(date_str, "%Y-%m-%d").date() for date_str in excluded_dates if date_str]
    return excluded_dates

def get_table_schema(table):
    table_prefix = ("l1_", "l2_", "l3_", "l4_", "l5_", "l6_")
    if any([table.startswith(prefix) for prefix in table_prefix]):
        table_schema = "c360_external" 
    else :
        table_schema = "c360_l0"
    return table_schema


def get_partition_col_name(table:str):
    partition_col_name = ("start_of_month", "partition_month", "start_of_week", "event_partition_date", "partition_date")
    table_schema = get_table_schema(table)
    if table.startswith("l1_"):
        common_values = ['event_partition_date']
    else :
        df = spark.sql(f"SELECT * FROM {table_schema}.{table} WHERE 1=0")
        column_names = df.columns
        common_values = list(set(partition_col_name).intersection(column_names))
    return common_values


def call_c360_by_partition(table:str, partition:str, partition_col_name:list, conditions_list:list):
    table_schema = get_table_schema(table)
    if re.match(r"^\d{8}$", partition):
        parsed_date = datetime.strptime(partition, "%Y%m%d")
        partition = parsed_date.strftime("%Y-%m-%d")

    if not conditions_list:
        df_condition_partition_filtered = spark.sql(f'select * from {table_schema}.{table} where {partition_col_name[0]} = "{partition}"')
    else:
        df_condition_partition_filtered = spark.sql(f'select * from {table_schema}.{table} where {partition_col_name[0]} = "{partition}"')
        df_condition_partition_filtered = df_condition_partition_filtered.filter(expr(conditions_list[0]))
    return df_condition_partition_filtered
