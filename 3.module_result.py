# Databricks notebook source
def add_rule_result_to_df(list_dict, partition_strings, all_partition_value, all_partition_rows, rules_filtered_df, col_name):
    pd_df = rules_filtered_df.toPandas()
    if len(list_dict) > 0:
        list_dict = [{k: [all_partition_value[j][i], all_partition_rows[j][i]] for j, k in enumerate(partition_strings)} for i, d in enumerate(list_dict)]
        pd_df[col_name] = list_dict
    else:
        pd_df = pd.DataFrame()
    return pd_df

def add_table_conditions(dq_rule_res_s, conditions_list):
    # If conditions_list is empty, set the 'Condition' column to None using lit
    if not conditions_list:
        dq_rule_res_s = dq_rule_res_s.withColumn("Condition", lit(None))
    else:
        # If conditions_list is not empty, use the first element from the list and use lit
        dq_rule_res_s = dq_rule_res_s.withColumn("Condition", lit(conditions_list[0]))
    return dq_rule_res_s

def sum_of_values(All_partition_good, index):
    if All_partition_good is None:
        return None
    res = 0
    for _, values in All_partition_good.items():
        value_at_index = values[index]
        if value_at_index is not None:
            res += value_at_index
        else:
            res = None
    return res

def save_df_to_databricks_by_granular(table_success_db, partition_success_db, table_fail_db, partition_fail_db):
    good_result_output.write.format("delta").mode("append").saveAsTable(table_success_db)
    good_result_output.write.format("delta").mode("append").saveAsTable("dg_dq_report.c360_external_dq_rule_success_report_all")
    history_good_result_output.write.format("delta").mode("append").saveAsTable(partition_success_db)
    error_result_output.write.format("delta").mode("append").saveAsTable(table_fail_db)
    error_result_output.write.format("delta").mode("append").saveAsTable("dg_dq_report.c360_external_dq_rule_fail_report_all")
    history_error_result_output.write.format("delta").mode("append").saveAsTable(partition_fail_db)
