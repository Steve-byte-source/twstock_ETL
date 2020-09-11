#!/usr/bin/env python
# coding: utf-8

from pyspark import SparkConf, SparkContext, sql
from pyspark.sql import SparkSession

## Import data from mysql to hdfs
# Changing path, IP,  user_name, user_password for your system

def transactions(date):
    sc = SparkContext()
    spark = SparkSession.builder\
                        .appName("Import_transactions")\
                        .getOrCreate()
    
    transactions = spark.read.jdbc(
                                   url = "jdbc:mysql://'IP':3306/'database_name'",
                                   table = f"(SELECT * FROM transactions WHERE Date = '{date}')AS my_table",
                                   properties = {'user': 'user_name', 'password': 'user_password'}
                                  )



    path = "hdfs://teb101-1.cloudera.com/user/admin/twstock/raw_data/transactions"
    transactions.write.parquet(f"{path}/{date}.parquet")
        
def broker_transaction(date):
    sc = SparkContext()
    spark = SparkSession.builder\
                        .appName("Import_broker_transaction")\
                        .getOrCreate()
    
    broker_transaction = spark.read.jdbc(
                                         url = "jdbc:mysql://'IP':3306/'database_name'",
                                         table = f"(SELECT * FROM broker_transaction WHERE Date = '{date}')AS my_table",
                                         properties = {'user': 'user_name', 'password': 'user_password'}
                                        )

    path = "hdfs://teb101-1.cloudera.com/user/admin/twstock/raw_data/broker_transaction"
    broker_transaction.write.parquet(f"{path}/{date}.parquet")

