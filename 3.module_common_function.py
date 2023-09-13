# Databricks notebook source
def myspark_to_list(df:DataFrame ,col_name: str):
    lists = df.select(col_name).collect()
    lists = [row[col_name] for row in lists]
    return lists
