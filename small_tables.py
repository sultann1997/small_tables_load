# -*- coding: cp1251 -*-
from checker import checker
import os, requests
import sys
from time import time

total_start_time = time()

small_tables = [
    {'table_name' : 'table1 name in dbase', 'dependence' : 'my_dep', 'url' : 'jdbc:oracle:thin:@test.example.com:port/dbase', 'schema' : 'my_schema'},
    {'table_name' : 'table2 name in dbase', 'dependence' : 'my_dep', 'url' : 'jdbc:oracle:thin:@test.example.com:port/dbase', 'schema' : 'my_schema', 
'query' : """My specific query to oracle/postgre""", 'hdfs_query' : "my query to get the same data from hdfs (due to different table namings)"},
    {'table_name' : 'table3 name in dbase', 'dependence' : 'my_dep', 'url' : 'jdbc:oracle:thin:@test.example.com:port/dbase2', 'schema' : 'different schema', 'fetchsize' : 1},
]
token = os.environ['my_token']

for table in small_tables:
    start_time = time()
    
    dependence = table['dependence']
    payload = requests.get(f'api with passwords/?token={token}&dependence={dependence}').json()
    username, password = payload['username'], payload['password']

    spark = checker.get_spark()
    table_name = table['table_name']
    print('Starting to load table:',table_name)
    url = table['url']
    if 'query' in table:
        query = table['query']
    else:
        query = f"""
        SELECT * FROM {table['schema']}.{table_name.upper()}
        """
    if 'fetchsize' in table:
        fetchsize = table['fetchsize']
    else:
        fetchsize = 10

    df = spark.read\
        .format('jdbc')\
        .option('url', url)\
        .option('query', query)\
        .option('fetchsize', fetchsize)\
        .option('user', username)\
        .option('password', password)\
        .option('driver', 'oracle.jdbc.driver.OracleDriver')\
        .load()
    hdfs_path = f'hdfs_path/{table_name}'
    print('Writing table to:', hdfs_path)
    df.write.mode('overwrite').parquet(hdfs_path)
    if 'hdfs_query' in table:
        hdfs_query  = table['hdfs_query']
    else:
        hdfs_query = None
    spark.sql(f'REFRESH TABLE HIVE_SCHEMA.{table_name}')
    #checking for data integrity
    checker.Checker(url, table_name, username, password, query, f"HIVE_SCHEMA.{table['table_name']}", schema=table['schema'], hdfs_query = hdfs_query).check_init()
    
    print('Finished table:', table_name)
    print('Time Taken:', time() - start_time, 'seconds\n')

print('Total Time Taken:', time() - total_start_time, 'seconds')

spark.stop()
sys.exit(0)
