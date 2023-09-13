# Databricks notebook source
def rule_clean(dq_rules_spark:DataFrame):
    rules_cleaned_df = dq_rules_spark.withColumn("Logic_SQL", when(col("Logic_spark").isNull(), col("Logic_SQL"))
                                                .otherwise(None))\
                                                .filter(col("Logic_spark").isNotNull() | col("Logic_SQL").isNotNull())
    return rules_cleaned_df

def run_sql(df:DataFrame, logic:str):
    """run sql command that store in sharepoint"""
    try:
      sql_value = df.filter(logic).count()
    except:
      sql_value = None
    return sql_value
  
def loadMaster(master_path:str, master_col:str):
    try:
        master_df = spark.read.format("csv") \
                            .option("header", "true") \
                            .load(master_path)
        master_list = myspark_to_list(master_df, master_col)
    except:
        master_list = [None]
    return master_list
  
def run_spark(df:DataFrame ,logic:str ,master_list:list):
    try:
      spark_value = eval(logic).count()
    except:
      spark_value = None
    return spark_value

def good_value_col(value_list:dict):
  if all(value is None for value in value_list.values()):
    result = None
  else:
    result = sum(value for value in value_list.values())
  return result

def select_sql_logic(table_rules_filtered_df):
    ##### Select only SQL logic
    sql_rules_filtered_df = table_rules_filtered_df.filter(col('Logic_SQL').isNotNull())
    list_sql_rule = myspark_to_list(sql_rules_filtered_df, 'Logic_SQL')
    print(f'Number of SQL rules: {len(list_sql_rule)}')
    list_sql_dict = myspark_to_list(sql_rules_filtered_df, "All_partition_good")
    return sql_rules_filtered_df, list_sql_rule, list_sql_dict

def select_spark_logic(table_rules_filtered_df):
    ##### Select only spark logic
    spark_rules_filtered_df = table_rules_filtered_df.filter(col('Logic_spark').isNotNull())
    list_spark_rule = myspark_to_list(spark_rules_filtered_df, 'Logic_spark')
    print(f'Number of spark rules: {len(list_spark_rule)}')
    list_spark_dict = myspark_to_list(spark_rules_filtered_df, "All_partition_good")

    # if we path master to check value in col is in master?
    list_master_path = myspark_to_list(spark_rules_filtered_df ,'Master_Path')
    list_master_col = myspark_to_list(spark_rules_filtered_df ,'Master_Col_Name')
    master_lists = [loadMaster(master_path, master_col) if master_path != None and master_col != None
                                                        else None
                                                        for master_path, master_col in zip(list_master_path, list_master_col)]
    print(f"    master path: {list_master_path}")
    print(f"    master col: {list_master_col}")
    print(f"    master list: {master_lists}")
    return spark_rules_filtered_df, list_spark_rule, list_spark_dict, list_master_path, list_master_col, master_lists

def apply_rules(df_partition_spark, partition_count_row, list_sql_rule, list_spark_rule, master_lists, all_partition_sql_value, all_partition_sql_rows, all_partition_spark_value, all_partition_spark_rows):
    if len(list_sql_rule) > 0:
        sql_value = [run_sql(df_partition_spark, logic) for logic in list_sql_rule]
        print(f"        good sql value of each rule: {sql_value}")
        partition_rows = [partition_count_row for logic in list_sql_rule]
        print(f"        good sql value of partition rows: {partition_rows}")
        all_partition_sql_value.append(sql_value)
        all_partition_sql_rows.append(partition_rows)

    if len(list_spark_rule) > 0:
        spark_value = [run_spark(df_partition_spark, logic, master_list) for logic, master_list in zip(list_spark_rule, master_lists)]
        print(f"        good spark value of each rule: {spark_value}")
        partition_rows = [partition_count_row for logic in list_spark_rule]
        print(f"        good spark value of partition rows: {partition_rows}")
        all_partition_spark_value.append(spark_value)
        all_partition_spark_rows.append(partition_rows)

def process_df(table, partition_strings, partition_col_name, conditions_list, list_sql_rule, list_spark_rule, master_lists, all_partition_sql_value, all_partition_sql_rows, all_partition_spark_value, all_partition_spark_rows):
    countpar = 0
    for partition in partition_strings:
        countpar += 1
        print(f'    Currently, processing the partition number: {countpar}/{len(partition_strings)}')
        print(f"    working on this partition: {partition}")
        df_partition_spark = call_c360_by_partition(table, partition, partition_col_name, conditions_list)
        df_partition_spark.persist()
        partition_count_row = df_partition_spark.count()
        print(f"      partition rows count: {partition_count_row}")

        apply_rules(df_partition_spark, partition_count_row, list_sql_rule, list_spark_rule, master_lists, all_partition_sql_value, all_partition_sql_rows, all_partition_spark_value, all_partition_spark_rows)
        
        df_partition_spark.unpersist()

    print(f"value of all sql rules: {all_partition_sql_value}")
    print(f"value of all partition sql rows: {all_partition_sql_rows}")
    print(f"value of all spark rules:{all_partition_spark_value}")
    print(f"value of all partition spark rows:{all_partition_spark_rows}")
