# Databricks notebook source
def combine_good_and_total_values(col1, col2):
    return [[col1[index1], col2[index2]] for index1, index2 in zip(range(len(col1)), range(len(col2)))]

def combine_date_and_values(col1, col2):
    return {col1[indexa]: col2[indexb] for indexa, indexb in zip(range(len(col1)), range(len(col2)))}

def no_more_run_partition(col, partition_strings):
    return {date: values for date, values in col.items() if date in partition_strings}

def want_to_run_partitions(col, partition_strings):
    return [date for date in partition_strings if date not in col.keys()]

def collect_latest_result(table_result_db:str, table_schema, partition_result_db:str, partition_schema):
    try:
        table_result = spark.sql("select * from {}".format(table_result_db))
        max_exec_date = table_result.select(max("Exec_date")).first()[0]
        latest_table_result = table_result.filter(col("Exec_date") == max_exec_date)
        latest_table_result = latest_table_result.select([latest_table_result[col].cast(table_schema[col].dataType) for col in latest_table_result.columns])

        partition_result = spark.sql("select * from {}".format(partition_result_db))
        max_exec_date_partition = partition_result.select(max("Exec_date")).first()[0]
        latest_partition_result = partition_result.filter(col("Exec_date") == max_exec_date_partition)
        latest_partition_result = latest_partition_result.select([latest_partition_result[col].cast(partition_schema[col].dataType) for col in latest_partition_result.columns])
    except:
        latest_table_result = spark.createDataFrame([], table_schema)
        latest_partition_result = spark.createDataFrame([], partition_schema)
    return latest_table_result, latest_partition_result
