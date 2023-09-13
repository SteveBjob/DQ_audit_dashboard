# Databricks notebook source
from pyspark.sql.functions import col, when, lit, max, expr, to_timestamp, date_format, isnan, collect_list, array, first, map_concat, to_json, from_json, map_filter, format_number, round, concat
from pyspark.sql.types import ArrayType, DateType, MapType, StructField, StringType, IntegerType, StructType, DoubleType, LongType
from pyspark.sql import DataFrame
from datetime import datetime, date,timedelta
from dateutil.relativedelta import relativedelta
import re

# COMMAND ----------

# MAGIC %md
# MAGIC ###Global

# COMMAND ----------

# MAGIC %run "/Users/chanwitt@ais.co.th/DQ_Audit_Dashboard_Report/incremental/3.module_common_function"

# COMMAND ----------

# MAGIC %md
# MAGIC ###Rule

# COMMAND ----------

# MAGIC %run "/Users/chanwitt@ais.co.th/DQ_Audit_Dashboard_Report/incremental/3.module_rules"

# COMMAND ----------

# MAGIC %md
# MAGIC ###c360 df

# COMMAND ----------

# MAGIC %run "/Users/chanwitt@ais.co.th/DQ_Audit_Dashboard_Report/incremental/3.module_c360_df"

# COMMAND ----------

# MAGIC %md
# MAGIC ##lastest run

# COMMAND ----------

# MAGIC %run "/Users/chanwitt@ais.co.th/DQ_Audit_Dashboard_Report/incremental/3.module_lastest_result"

# COMMAND ----------

# MAGIC %md
# MAGIC ##Result

# COMMAND ----------

# MAGIC %run "/Users/chanwitt@ais.co.th/DQ_Audit_Dashboard_Report/incremental test/3.module_result"

# COMMAND ----------

# MAGIC %md
# MAGIC ##body

# COMMAND ----------

exec_date = datetime.today().strftime("%Y-%m-%d")
print(f"execute date: {exec_date}")

# create and clean conditions df
#null not include
dq_conditions_spark = dq_conditions_spark.filter(col("Condition").isNotNull())
print("-----------------------------------------this is clean conditions table")
dq_conditions_spark.display()

# create and clean rules df
#create col Rule_Name 
rules_clean_spark = rule_clean(dq_rules_spark)
rules_clean_spark = rules_clean_spark.withColumn("Exec_date", lit(exec_date))
rules_clean_spark = rules_clean_spark.withColumn("Rule_Name", concat(col("Table_name"), lit("."), col("Column"), lit("."), col("Dimension")))
print("-----------------------------------------this is clean rules table")
rules_clean_spark.display()

# list of all table that we have
table_lists = myspark_to_list(rules_clean_spark ,'Table_name')
table_lists = list(set(table_lists))
print(table_lists)

# find partition
max_exec_date = all_table_partitions.select(max("exec_date")).first()[0]
all_table_partitions = all_table_partitions.filter(col("exec_date") == max_exec_date)
my_latest_partitions = all_table_partitions.filter(col("Table_name").isin(table_lists))

# Register the lambda UDF
parse_excluded_dates_udf = udf(
    lambda excluded_dates: [
        datetime.strptime(date_str, "%Y-%m-%d").date() for date_str in excluded_dates.strip("[]").split(" | ") if date_str
    ],
    ArrayType(DateType())
)
my_latest_partitions = my_latest_partitions.withColumn(
    "excluded_dates_list",
    parse_excluded_dates_udf(col("loss_date"))
)
generate_dates_udf = udf(generate_dates, ArrayType(DateType()))
# Create a new column "all_partition" using the UDF and the parsed excluded_dates
my_latest_partitions = my_latest_partitions.withColumn("All_partition", generate_dates_udf(col("start_date"), col("lastest_date"), col("excluded_dates_list")))
print("-----------------------------------------this is clean my_latest_partitions")
my_latest_partitions.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Previous table

# COMMAND ----------

previous_table_schema = StructType([
    StructField("Domain", StringType(), True),
    StructField("Level", StringType(), True),
    StructField("Table_name", StringType(), True),
    StructField("Column", StringType(), True),
    StructField("Rule_Name", StringType(), True),
    StructField("Condition", StringType(), True),
    StructField("Business_Rule", StringType(), True),
    StructField("Dimension", StringType(), True),
    StructField("Master_Path", StringType(), True),
    StructField("Master_Col_Name", StringType(), True),
    StructField("Logic_spark", StringType(), True),
    StructField("Logic_SQL", StringType(), True),
    StructField("OWNER", StringType(), True),
    StructField("log", StringType(), True),
    StructField("start_date", DateType(), True),
    StructField("lastest_date", DateType(), True),
    StructField("Good_value", LongType(), True),
    StructField("Bad_value", LongType(), True),
    StructField("Total_value", LongType(), True),
    StructField("%Good_value", DoubleType(), True),
    StructField("%Bad_value", DoubleType(), True),
    StructField("Exec_date", DateType(), True)
])

previous_partitions_schema = StructType([
    StructField("Domain", StringType(), True),
    StructField("Level", StringType(), True),
    StructField("Table_name", StringType(), True),
    StructField("Column", StringType(), True),
    StructField("Logic_spark", StringType(), True),
    StructField("Logic_SQL", StringType(), True),
    StructField("Rule_Name", StringType(), True),
    StructField("Exec_date", DateType(), True),
    StructField("All_partition", StringType(), True),
    StructField("Good_value", IntegerType(), True),
    StructField("Total_value", IntegerType(), True),
    StructField("Bad_value", IntegerType(), True),
    StructField("%Good_value", DoubleType(), True),
    StructField("%Bad_value", DoubleType(), True)
])

latest_table_result, latest_partition_result = collect_latest_result(table_success_db, previous_table_schema, partition_success_db, previous_partitions_schema)
latest_table_result.display()
latest_partition_result.display()
previous_tables_result = spark.createDataFrame([], previous_table_schema)
previous_partitions_result = spark.createDataFrame([], previous_partitions_schema)

#partition history
lastest_run_combine_raw = latest_partition_result.groupby("Rule_Name","Table_name").agg(collect_list("All_partition"),collect_list("Good_value"),collect_list("Total_value"))
# Register the UDF
combine_good_and_total_values_udf = udf(combine_good_and_total_values, ArrayType(ArrayType(IntegerType())))
# Apply the UDF to the DataFrame
lastest_run_combine_raw = lastest_run_combine_raw.withColumn(
    "combined_values",
    combine_good_and_total_values_udf(col("collect_list(Good_value)"), col("collect_list(Total_value)"))
)
# Register the UDF
combine_date_and_values_udf = udf(combine_date_and_values, MapType(StringType(), ArrayType(IntegerType())))
# Apply the UDF to the DataFrame
lastest_run_combine_raw = lastest_run_combine_raw.withColumn(
    "dict_date_values",
    combine_date_and_values_udf(col("collect_list(All_partition)"), col("combined_values"))
)
# lastest_run_combine_raw = lastest_run_combine_raw.withColumnRenamed("All_partition", "All_partition_good")

print("-----------------------------------------this is my partitions lastest_run")
lastest_run_combine_raw.display()

# COMMAND ----------

# Define the schema of the empty DataFrame
schema = StructType([
    StructField("Domain", StringType(), nullable=False),
    StructField("Level", StringType(), nullable=False),
    StructField("Table_name", StringType(), nullable=False),
    StructField("Column", StringType(), nullable=False),
    StructField("Rule_Name", StringType(), nullable=False),
    StructField("Business_Rule", StringType(), nullable=False),
    StructField("Dimension", StringType(), nullable=False),
    StructField("Master_Path", StringType(), nullable=True),
    StructField("Master_Col_Name", StringType(), nullable=True),
    StructField("Logic_spark", StringType(), nullable=True),
    StructField("Logic_SQL", StringType(), nullable=False),
    StructField("OWNER", StringType(), nullable=True),
    StructField("log", StringType(), nullable=True),
    StructField("Exec_date", DateType(), nullable=False),
    StructField("All_partition_good", MapType(StringType(), ArrayType(LongType(), containsNull=True)), nullable=True),
    StructField("start_date", DateType(), nullable=False),
    StructField("lastest_date", DateType(), nullable=False),
    StructField("Condition", StringType(), nullable=True)
])

# Create an empty DataFrame with the defined schema
result = spark.createDataFrame([], schema)

# COMMAND ----------

no_data_in_master_active = []
count = 0

for table in table_lists:
    count += 1
    print(f'Currently, processing the table number: {count}/{len(table_lists)}')
    print(f'right now we are working on table: {table}')

    # table partition col name
    partition_col_name = get_partition_col_name(table)
    print(f"name of partition col: {partition_col_name}")

    # find condition list on table
    table_condition_spark = dq_conditions_spark.filter(col('Table_name') == table)
    conditions_list = myspark_to_list(table_condition_spark ,'Condition')
    print(f'table condition: {conditions_list}')

    # find start_date and lastest_date
    my_latest_partitions_table_spark = my_latest_partitions.filter(col("table_name") == table)
    start_date = myspark_to_list(my_latest_partitions_table_spark, "start_date")
    lastest_date = myspark_to_list(my_latest_partitions_table_spark, "lastest_date")
    start_date_strings = [partition.strftime("%Y-%m-%d") for partition in start_date][0]
    lastest_date_strings = [partition.strftime("%Y-%m-%d") for partition in lastest_date][0]
    print(f"start partition: {start_date_strings}")
    print(f"lastest partition: {lastest_date_strings}")

    if my_latest_partitions_table_spark.count() == 0:
        print("No query results found.")
        no_data_in_master_active.append(table)

    else:
        print("--------Let fucking go boii--------")
        # find all partition of table and Convert the datetime.date objects to strings with the desired format
        partition_lists = myspark_to_list(my_latest_partitions_table_spark , "All_partition")
        partition_strings = [partition.strftime("%Y-%m-%d") for partition in partition_lists[0]]##################################################################################### dont forget this when doing in product use full partition_strings
        print(f"today partitions of this table: {partition_strings}")

        # all rule that apply to this table
        table_rules_spark = rules_clean_spark.filter(col('Table_name') == table)
        # Define a UDF to map each date to a dictionary with None as value
        date_dict_udf = udf(lambda: {date: None for date in partition_strings}, MapType(StringType(), IntegerType()))
        # Add a new column using the UDF
        table_rules_filtered_df = table_rules_spark.withColumn("All_partition_good", date_dict_udf())

        ##### Select only SQL logic
        sql_rules_filtered_df, list_sql_rule, list_sql_dict = select_sql_logic(table_rules_filtered_df)

        ##### Select only spark logic
        spark_rules_filtered_df, list_spark_rule, list_spark_dict, list_master_path, list_master_col, master_lists = select_spark_logic(table_rules_filtered_df)
        
        # find lastest partition of table
        try:
            lastest_run_combine = lastest_run_combine_raw.filter(col('Table_name') == table)
            have_value = lastest_run_combine.count()
            print(have_value)
        except Exception as e:
            have_value = 0

        if have_value == 0:
            print("New table")
            #find all value of sql and spark in every partitions
            all_partition_sql_value = []
            all_partition_spark_value = []
            all_partition_sql_rows = []
            all_partition_spark_rows = []
            process_df(table, partition_strings, partition_col_name, conditions_list, list_sql_rule, list_spark_rule, master_lists, all_partition_sql_value, all_partition_sql_rows, all_partition_spark_value, all_partition_spark_rows)

            pd_sql_rules_filtered_df = add_rule_result_to_df(list_sql_dict, partition_strings, all_partition_sql_value, all_partition_sql_rows, sql_rules_filtered_df, 'All_partition_good')
            pd_spark_rules_filtered_df = add_rule_result_to_df(list_spark_dict, partition_strings, all_partition_spark_value, all_partition_spark_rows, spark_rules_filtered_df, 'All_partition_good')

            #combine 2 logic
            dq_rule_res = pd.concat([pd_sql_rules_filtered_df, pd_spark_rules_filtered_df])
            dq_rule_res_ps = ps.from_pandas(dq_rule_res)
            dq_rule_res_s = dq_rule_res_ps.to_spark()
            # Convert struct column to map column
            dq_rule_res_s = dq_rule_res_s.withColumn('All_partition_good', from_json(to_json(col('All_partition_good')), 'map<string,array<long>>'))
            # Adding columns with Spark DataFrame operations and lit function
            dq_rule_res_s = dq_rule_res_s.withColumn("start_date", lit(start_date_strings))
            dq_rule_res_s = dq_rule_res_s.withColumn("lastest_date", lit(lastest_date_strings))

            dq_rule_res_s = add_table_conditions(dq_rule_res_s, conditions_list)
            # print("====================================this is new table==================================")
            # dq_rule_res_s.display()
            # dq_rule_res_s.printSchema()
            result = result.unionByName(dq_rule_res_s)

        else :
            print("Old table")
            # find partition that lastest run 
            # Register the UDF
            no_more_run_partition_udf = udf(no_more_run_partition, MapType(StringType(), ArrayType(IntegerType())))
            lastest_run_combine = lastest_run_combine.withColumn(
                "no_more_run_partition",
                no_more_run_partition_udf(col("dict_date_values"), array([lit(value) for value in partition_strings]))
            )
            #find partition that want to run
            # Register the UDF
            want_to_run_partitions_udf = udf(want_to_run_partitions, ArrayType(StringType()))
            lastest_run_combine = lastest_run_combine.withColumn(
                "want_to_run_partitions",
                want_to_run_partitions_udf(col("dict_date_values"), array([lit(value) for value in partition_strings]))
            )
            # lastest_run_combine.display()
            # Collect the first row of the "want_to_run_partitions" column
            first_partition  = lastest_run_combine.select(first(col("want_to_run_partitions"))).first()
            run_every_rule_partition = first_partition[0] if first_partition is not None else []
            if run_every_rule_partition is None:
                run_every_rule_partition = []
            print(f"    partitions that didn't run from previous: {run_every_rule_partition}")
            run_new_rule_partition = [partition for partition in partition_strings if partition not in (run_every_rule_partition or [])]
            # print(f"    partitions that new rule have to run: {run_new_rule_partition}")
            if run_every_rule_partition == []:
                print(" Old partition")
                print(">>>>>>>>>>>>>>>>>Catch previous value<<<<<<<<<<<<<<<<<<<<<<")

                previous_tables_filtered = latest_table_result.filter(col("Table_name") == table)
                previous_partitions_filtered = latest_partition_result.filter(col("Table_name") == table)
                previous_tables_result = previous_tables_result.unionByName(previous_tables_filtered)
                previous_partitions_result = previous_partitions_result.unionByName(previous_partitions_filtered)

                print(">>>>>>>>>>>>>>>>>Check is it have new rules<<<<<<<<<<<<<<<<<<<<<<")
                previous_tables_filtered = previous_tables_filtered.select("Rule_Name", "Table_name").withColumnRenamed("Table_name", "Table_name2")
                result_df = table_rules_filtered_df.join(previous_tables_filtered, on='Rule_Name', how='left')
                filtered_result_df = result_df.filter(col("Table_name2").isNull())
                have_new_rules = filtered_result_df.count()
                filtered_result_df = filtered_result_df.drop("Table_name2")
                if have_new_rules == 0:
                    print(">>>>>>>>>>>>>>>>>no new rules go next<<<<<<<<<<<<<<<<<<<<<<")
                    print("=========================================================================================================================================================================================")

                else :
                    print("new rules")

                    ##### Select only SQL logic
                    sql_rules_filtered_df, list_sql_rule, list_sql_dict = select_sql_logic(filtered_result_df)
                    print(f"list_sql_dict: {list_sql_dict}")
                    
                    ##### Select only spark logic
                    spark_rules_filtered_df, list_spark_rule, list_spark_dict, list_master_path, list_master_col, master_lists = select_spark_logic(filtered_result_df)
                    print(f"list_spark_dict: {list_spark_dict}")

                    #find all value of sql and spark in every partitions
                    all_partition_sql_value = []
                    all_partition_spark_value = []
                    all_partition_sql_rows = []
                    all_partition_spark_rows = []
                    process_df(table, partition_strings, partition_col_name, conditions_list, list_sql_rule, list_spark_rule, master_lists, all_partition_sql_value, all_partition_sql_rows, all_partition_spark_value, all_partition_spark_rows)
                    
                    pd_sql_rules_filtered_df = add_rule_result_to_df(list_sql_dict, partition_strings, all_partition_sql_value, all_partition_sql_rows, sql_rules_filtered_df, "All_partition_good")
                    
                    pd_spark_rules_filtered_df = add_rule_result_to_df(list_spark_dict, partition_strings, all_partition_spark_value, all_partition_spark_rows, spark_rules_filtered_df, "All_partition_good")

                    #combine 2 logic
                    dq_rule_res = pd.concat([pd_sql_rules_filtered_df, pd_spark_rules_filtered_df])
                    dq_rule_res_ps = ps.from_pandas(dq_rule_res)
                    dq_rule_res_s = dq_rule_res_ps.to_spark()
                    # Convert struct column to map column
                    dq_rule_res_s = dq_rule_res_s.withColumn('All_partition_good', from_json(to_json(col('All_partition_good')), 'map<string,array<long>>'))
                    # Adding columns with Spark DataFrame operations and lit function
                    dq_rule_res_s = dq_rule_res_s.withColumn("start_date", lit(start_date_strings))
                    dq_rule_res_s = dq_rule_res_s.withColumn("lastest_date", lit(lastest_date_strings))

                    dq_rule_res_s = add_table_conditions(dq_rule_res_s, conditions_list)
                    # print("====================================this is new table==================================")
                    # dq_rule_res_s.display()
                    # dq_rule_res_s.printSchema()
                    result = result.unionByName(dq_rule_res_s)
                # Skip the rest of the loop body
                continue
            
            #find all value of sql and spark in every partitions
            all_new_partition_sql_value = []
            all_new_partition_spark_value = []
            all_new_partition_sql_rows = []
            all_new_partition_spark_rows = []
            process_df(table, run_every_rule_partition, partition_col_name, conditions_list, list_sql_rule, list_spark_rule, master_lists, all_new_partition_sql_value, all_new_partition_sql_rows, all_new_partition_spark_value, all_new_partition_spark_rows)

            pd_sql_rules_filtered_df = add_rule_result_to_df(list_sql_dict, run_every_rule_partition, all_new_partition_sql_value, all_new_partition_sql_rows, sql_rules_filtered_df, "All_new_partition_good")
            pd_spark_rules_filtered_df = add_rule_result_to_df(list_spark_dict, run_every_rule_partition, all_new_partition_spark_value, all_new_partition_spark_rows, spark_rules_filtered_df, "All_new_partition_good")
            
            #combine 2 logic
            dq_new_partition_rule_res_pd = pd.concat([pd_sql_rules_filtered_df, pd_spark_rules_filtered_df])

            dq_new_partition_rule_res_pds = ps.from_pandas(dq_new_partition_rule_res_pd)
            dq_new_partition_rule_res_s = dq_new_partition_rule_res_pds.to_spark()
            # dq_new_partition_rule_res_s.display()
            # Perform the join
            lastest_run_combine_filtered = lastest_run_combine.select(col('Rule_Name'),col('dict_date_values'))
            dq_new_partition_rule_res_s = dq_new_partition_rule_res_s.join(lastest_run_combine_filtered, on="Rule_Name", how="left")

            filtered_dq_old_partition_rule_res_s = dq_new_partition_rule_res_s.filter(col('dict_date_values').isNull())
            count_filtered_dq_old_partition_rule_res_s = filtered_dq_old_partition_rule_res_s.count()
            if count_filtered_dq_old_partition_rule_res_s != 0:
                print("new rules")
                ##select col
                ##### Select only SQL logic
                sql_rules_filtered_df, list_sql_rule, list_sql_dict = select_sql_logic(filtered_dq_old_partition_rule_res_s)

                ##### Select only spark logic
                spark_rules_filtered_df, list_spark_rule, list_spark_dict, list_master_path, list_master_col, master_lists = select_spark_logic(filtered_dq_old_partition_rule_res_s)

                print(f"    partitions that new rule have to run: {run_new_rule_partition}")
                all_old_partition_sql_value = []
                all_old_partition_spark_value = []
                all_old_partition_sql_rows = []
                all_old_partition_spark_rows = []
                process_df(table, run_new_rule_partition, partition_col_name, conditions_list, list_sql_rule, list_spark_rule, master_lists, all_old_partition_sql_value, all_old_partition_sql_rows, all_old_partition_spark_value, all_old_partition_spark_rows)

                pd_sql_rules_filtered_df = add_rule_result_to_df(list_sql_dict, run_new_rule_partition, all_old_partition_sql_value, all_old_partition_sql_rows, sql_rules_filtered_df, "dict_date_values2")
                pd_spark_rules_filtered_df = add_rule_result_to_df(list_spark_dict, run_new_rule_partition, all_old_partition_spark_value, all_old_partition_spark_rows, spark_rules_filtered_df, "dict_date_values2")

                #combine 2 logic
                dq_old_partition_rule_res_pd = pd.concat([pd_sql_rules_filtered_df, pd_spark_rules_filtered_df])
                dq_old_partition_rule_res_pd = dq_old_partition_rule_res_pd[['Rule_Name','dict_date_values2']]
                dq_old_partition_rule_res_pds = ps.from_pandas(dq_old_partition_rule_res_pd)
                dq_old_partition_rule_res_s = dq_old_partition_rule_res_pds.to_spark()
                # dq_old_partition_rule_res_s.display()
                dq_new_partition_rule_res_s = dq_new_partition_rule_res_s.join(dq_old_partition_rule_res_s, on="Rule_Name", how="outer")

            # Convert struct column to map column
            dq_new_partition_rule_res_s = dq_new_partition_rule_res_s.withColumn('All_new_partition_good', from_json(to_json(col('All_new_partition_good')), 'map<string,array<long>>'))
            # Check if the column 'dict_date_values2' exists in the DataFrame
            if 'dict_date_values2' in dq_new_partition_rule_res_s.columns:
                # Perform the transformation using from_json and to_json
                dq_new_partition_rule_res_s = dq_new_partition_rule_res_s.withColumn(
                    'dict_date_values2',
                    from_json(to_json(col('dict_date_values2')), 'map<string,array<long>>')
                )
                # Check if 'dict_date_values' is null and update it from 'dict_date_values2'
                dq_new_partition_rule_res_s = dq_new_partition_rule_res_s.withColumn(
                    'dict_date_values',
                    when(col('dict_date_values').isNull(), col('dict_date_values2')).otherwise(col('dict_date_values'))
                )

            # Combine the map columns using map_concat
            dq_new_partition_rule_res_s = dq_new_partition_rule_res_s.withColumn('value3', map_concat(dq_new_partition_rule_res_s['All_new_partition_good'], dq_new_partition_rule_res_s['dict_date_values']))
            dq_new_partition_rule_res_s = dq_new_partition_rule_res_s.filter(col("dict_date_values").isNotNull())
            # Filter the key-value pairs based on the partition_strings list
            dq_new_partition_rule_res_s = dq_new_partition_rule_res_s.withColumn('All_partition_good', map_filter(dq_new_partition_rule_res_s['value3'], lambda k, v: k.isin(partition_strings)))
            # Drop the columns from the DataFrame
            cols_to_drop = ["value3", "dict_date_values2","dict_date_values","All_new_partition_good"]
            dq_new_partition_rule_res_s = dq_new_partition_rule_res_s.drop(*cols_to_drop)

            # Adding columns with Spark DataFrame operations and lit function
            dq_new_partition_rule_res_s = dq_new_partition_rule_res_s.withColumn("start_date", lit(start_date_strings))
            dq_new_partition_rule_res_s = dq_new_partition_rule_res_s.withColumn("lastest_date", lit(lastest_date_strings))

            dq_new_partition_rule_res_s = add_table_conditions(dq_new_partition_rule_res_s, conditions_list)
            # print("====================================this is old table==================================")
            # dq_new_partition_rule_res_s.display()
            # dq_new_partition_rule_res_s.printSchema()
            result = result.unionByName(dq_new_partition_rule_res_s)

    print("=========================================================================================================================================================================================")

spark.conf.set("spark.sql.mapKeyDedupPolicy", "LAST_WIN")

previous_tables_result = previous_tables_result.withColumn("Exec_date", lit(exec_date))
previous_tables_result.display()
previous_partitions_result = previous_partitions_result.withColumn("Exec_date", lit(exec_date))
previous_partitions_result.display()
result.display()

# COMMAND ----------

# Register the UDF with Spark
sum_of_values_udf = udf(sum_of_values, LongType())

result = result.withColumn("Good_value", sum_of_values_udf("All_partition_good", lit(0)))\
                .withColumn("Total_value", sum_of_values_udf("All_partition_good", lit(1)))\
                .withColumn("Bad_value", col("Total_value") - col("Good_value"))\
                .withColumn("%Good_value", format_number((col("Good_value") * 100 / col("Total_value")), 2))\
                .withColumn("%Bad_value", format_number((col("Bad_value") * 100 / col("Total_value")), 2))

# Define the desired column order
column_order = [
    'Domain',
    'Level',
    'Table_name',
    'Column',
    'Rule_Name',
    'Condition',
    'Business_Rule',
    'Dimension',
    'Master_Path',
    'Master_Col_Name',
    'Logic_spark',
    'Logic_SQL',
    'OWNER',
    'log',
    'All_partition_good',
    'start_date',
    'lastest_date',
    'Good_value',
    'Bad_value',
    'Total_value',
    '%Good_value',
    '%Bad_value',
    'Exec_date'
]
result = result.select(*column_order)

schema = StructType([
    StructField("Domain", StringType(), nullable=True),
    StructField("Level", StringType(), nullable=True),
    StructField("Table_name", StringType(), nullable=True),
    StructField("Column", StringType(), nullable=True),
    StructField("Rule_Name", StringType(), nullable=True),
    StructField("Condition", StringType(), nullable=True),
    StructField("Business_Rule", StringType(), nullable=True),
    StructField("Dimension", StringType(), nullable=True),
    StructField("Master_Path", StringType(), nullable=True),
    StructField("Master_Col_Name", StringType(), nullable=True),
    StructField("Logic_spark", StringType(), nullable=True),
    StructField("Logic_SQL", StringType(), nullable=True),
    StructField("OWNER", StringType(), nullable=True),
    StructField("log", StringType(), nullable=True),
    StructField("All_partition_good", MapType(StringType(), ArrayType(IntegerType(), containsNull=True))),
    StructField("start_date", DateType(), nullable=True),
    StructField("lastest_date", DateType(), nullable=True),
    StructField("Good_value", LongType(), nullable=True),
    StructField("Bad_value", LongType(), nullable=True),
    StructField("Total_value", LongType(), nullable=True),
    StructField("%Good_value", DoubleType(), nullable=True),
    StructField("%Bad_value", DoubleType(), nullable=True),
    StructField("Exec_date", DateType(), nullable=True)
])

# Convert columns to DateType
result = result.select([result[col].cast(schema[col].dataType) for col in result.columns])

for column in result.columns:
    # Replace NaN values with null in the current column
    try:
        result = result.withColumn(column, when(isnan(col(column)), None).otherwise(col(column)))
    except:
        continue

result.display()

good_result = result.filter(
                                  col('Good_value').isNotNull() &
                                  col('Rule_Name').isNotNull() &
                                  col('Business_Rule').isNotNull() &
                                  col('Dimension').isNotNull()
                                  )

# Filter rows where 'Good_value' is null
error_result = result.filter(
                                    col('Good_value').isNull() |
                                    col('Rule_Name').isNull() |
                                    col('Business_Rule').isNull() |
                                    col('Dimension').isNull()
                                    )
print('This is success value table')
good_result.display()
print('This is fail value table')
error_result.display()

# COMMAND ----------

# Explode the 'All_partition_good' dictionary column and split it into separate columns
history_good_result = good_result.selectExpr(
    "Domain",
    "Level",
    "Table_name",
    "Column",
    "Logic_spark",
    "Logic_SQL",
    "Rule_Name",
    "Exec_date",
    "explode(All_partition_good) as (All_partition, All_partition_good)"
)

# Split 'All_partition_good' dictionary column into two columns: 'Good_value' and 'Total_value'
history_good_result = history_good_result.withColumn("Good_value", history_good_result["All_partition_good"][0]) \
                         .withColumn("Total_value", history_good_result["All_partition_good"][1])

# Calculate 'Bad_value', '%Good_value', and '%Bad_value'
history_good_result = history_good_result.withColumn("Bad_value", col("Total_value") - col("Good_value")) \
                         .withColumn("%Good_value", ((col("Good_value")/ col("Total_value"))*100).cast("double")) \
                         .withColumn("%Bad_value", ((col("Bad_value")/ col("Total_value"))*100).cast("double")) \
                         .withColumn("%Good_value", round(col("%Good_value"), 2)) \
                         .withColumn("%Bad_value", round(col("%Bad_value"), 2))

good_result_output = good_result.drop("All_partition_good")
good_result_output = good_result_output.unionByName(previous_tables_result)
good_result_output.display()
history_good_result_output = history_good_result.drop("All_partition_good")
history_good_result_output = history_good_result_output.unionByName(previous_partitions_result)
history_good_result_output.display()

# COMMAND ----------

# Explode the 'All_partition_good' dictionary column and split it into separate columns
history_error_result = error_result.selectExpr(
    "Domain",
    "Level",
    "Table_name",
    "Column",
    "Logic_spark",
    "Logic_SQL",
    "Rule_Name",
    "Exec_date",
    "explode(All_partition_good) as (All_partition, All_partition_good)"
)

# Split 'All_partition_good' dictionary column into two columns: 'Good_value' and 'Total_value'
history_error_result = history_error_result.withColumn("Good_value", history_error_result["All_partition_good"][0]) \
                         .withColumn("Total_value", history_error_result["All_partition_good"][1])

# Calculate 'Bad_value', '%Good_value', and '%Bad_value'
history_error_result = history_error_result.withColumn("Bad_value", col("Total_value") - col("Good_value")) \
                         .withColumn("%Good_value", ((col("Good_value")/ col("Total_value"))*100).cast("double")) \
                         .withColumn("%Bad_value", ((col("Bad_value")/ col("Total_value"))*100).cast("double")) \
                         .withColumn("%Good_value", round(col("%Good_value"), 2)) \
                         .withColumn("%Bad_value", round(col("%Bad_value"), 2))

error_result_output = error_result.drop("All_partition_good")
error_result_output.display()
history_error_result_output = history_error_result.drop("All_partition_good")
history_error_result_output.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #set table schema

# COMMAND ----------

# Define the schema
partition_schema = StructType([
    StructField("Domain", StringType(), nullable=True),
    StructField("Level", StringType(), nullable=True),
    StructField("Table_name", StringType(), nullable=True),
    StructField("Column", StringType(), nullable=True),
    StructField("Logic_spark", StringType(), nullable=True),
    StructField("Logic_SQL", StringType(), nullable=True),
    StructField("Rule_Name", StringType(), nullable=True),
    StructField("Exec_date", DateType(), nullable=True),
    StructField("All_partition", DateType(), nullable=True),
    StructField("Good_value", IntegerType(), nullable=True),
    StructField("Total_value", IntegerType(), nullable=True),
    StructField("Bad_value", IntegerType(), nullable=True),
    StructField("%Good_value", DoubleType(), nullable=True),
    StructField("%Bad_value", DoubleType(), nullable=True)
])

# Define the schema
table_schema = StructType([
    StructField("Domain", StringType(), nullable=True),
    StructField("Level", StringType(), nullable=True),
    StructField("Table_name", StringType(), nullable=True),
    StructField("Column", StringType(), nullable=True),
    StructField("Rule_Name", StringType(), nullable=True),
    StructField("Condition", StringType(), nullable=True),
    StructField("Business_Rule", StringType(), nullable=True),
    StructField("Dimension", StringType(), nullable=True),
    StructField("Master_Path", StringType(), nullable=True),
    StructField("Master_Col_Name", StringType(), nullable=True),
    StructField("Logic_spark", StringType(), nullable=True),
    StructField("Logic_SQL", StringType(), nullable=True),
    StructField("OWNER", StringType(), nullable=True),
    StructField("log", StringType(), nullable=True),
    StructField("start_date", DateType(), nullable=True),
    StructField("lastest_date", DateType(), nullable=True),
    StructField("Good_value", LongType(), nullable=True),
    StructField("Bad_value", LongType(), nullable=True),
    StructField("Total_value", LongType(), nullable=True),
    StructField("%Good_value", DoubleType(), nullable=True),
    StructField("%Bad_value", DoubleType(), nullable=True),
    StructField("Exec_date", DateType(), nullable=True)
])


# Convert columns to DateType
good_result_output = good_result_output.select([good_result_output[col].cast(table_schema[col].dataType) for col in good_result_output.columns])
history_good_result_output = history_good_result_output.select([history_good_result_output[col].cast(partition_schema[col].dataType) for col in history_good_result_output.columns])
error_result_output = error_result_output.select([error_result_output[col].cast(table_schema[col].dataType) for col in error_result_output.columns])
history_error_result_output = history_error_result_output.select([history_error_result_output[col].cast(partition_schema[col].dataType) for col in history_error_result_output.columns])

# good_result_output = spark.createDataFrame(good_result_output, schema=table_schema)
# history_good_result_output = spark.createDataFrame(history_good_result_output, schema=partition_schema)
# error_result_output = spark.createDataFrame(error_result_output, schema=table_schema)
# history_error_result_output = spark.createDataFrame(history_error_result_output, schema=partition_schema)

# COMMAND ----------


