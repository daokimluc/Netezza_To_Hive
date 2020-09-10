import threading
from datetime import datetime
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import *
from datetime import date
import os
import sys
import json
from pyspark.sql import SQLContext, HiveContext,SparkSession
from pyspark import SparkContext, SparkConf
# Environment variable in windown
# path='D:\Spark\sqlServerToHive_pyspark\env_files\env.sh'
# load_dotenv(dotenv_path=path,verbose=True)

def main():
    config_file = sys.argv[1].lower()
    print(config_file)
   # section = sys.argv[2].lower()
    if config_file is None:
        sys.exit(1)
    else:
        spark_config(config_file)
        
def log(type, msg):
    print("{}:{}: {}".format(str(datetime.now()), type.upper(), msg))


def parse_config(config_file):
    f = json.loads(open(config_file).read())
    return f['database'], f['hostname'], f['port'], f['table_list'], f['icol'], f['partition']

def spark_config(config_file):
    appName = "PySpark SqlServer query Load"
    master = "local"
    spark = SparkSession.builder \
        .config('spark.scheduler.mode','FAIR') \
        .config("spark.sql.parquet.writeLegacyFormat", "true") \
        .config('spark.driver.memory', '8g') \
        .config('spark.executor.cores', 4) \
        .config('spark.executor.memory', '8g') \
        .config('spark.dynamicAllocation.enabled', True) \
        .config('spark.dynamicAllocation.maxExecutors', 25) \
        .config("spark.ui.port","4041") \
        .config('spark.scheduler.mode', 'FAIR') \
        .config('spark.executor.heartbeatInterval', 200000) \
        .config('spark.network.timeout',300000) \
        .appName(appName).enableHiveSupport().getOrCreate()
    sc = SparkContext.getOrCreate()
    # Run miltiple job
    databases, server, port, table_list, icol, partition = parse_config(
            config_file)
    for table in table_list:
        t = threading.Thread(target=spark_load, args=(config_file,table,spark,sc))
        t.start()
        # t.join()
        print('spark task', table, 'has started')
def spark_load(config_file,table,spark,sc):
        sc.setLocalProperty("spark.scheduler.pool", str(table))
        # for table in table_list:
        databases, server, port, table_list, icol, partition = parse_config(
            config_file)
        print(port, databases,table_list)
        # Reading Environment parameters
        hive_db = 'customer12' #os.environ['hive_db']
        user = 'sa'#os.environ['user']
        password = 123#os.environ['password']
        target_path = 'D:/sqlserver' #os.environ['target_path']
        op_format = 'parquet' #os.environ['format']
        op_mode = 'append' #os.environ['mode']
        for database in databases:
            log("info", "pyspark script to extract data from sqlServer server is starting. Time")
            print("-----------------------------------------------------------------------------------------------------------------")
            print("Server Name      : " + server)
            print("Port Number      : " + str(port))
            print("User Name        : " + user)
            print("Password         : " + str(password))
            print("Source database  : " + database)
            print("hive database    : " + hive_db)
            print("Destination path : " + target_path)
            print("Output Format    : " + op_format)
            print("Mode of Output   : " + op_mode)
            print("Table List       : " + str(table_list))
            print("Incr. Column     : " + icol)
            print("-----------------------------------------------------------------------------------------------------------------")
            success_file = '{}_success_{}.list'.format(str(date.today()), database)
            s_file = open(success_file, 'a')
            if table not in open(success_file).read():
                log("info", "**** Running for Table {} ***".format(table))
                mssqlDF = spark.read.format("jdbc").option("url", "jdbc:sqlserver://" + server + ":" + str(port) + ";databaseName=" + database).option("dbtable", table).option(
                    "user", user).option("password", password).option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
                # mssqlDF = spark.read.format("jdbc").option("url", "jdbc:sqlserver://" + server + ":" + str(port) + ";databaseName=" + database).option("dbtable", table).option(
                #     "user", user).option("password", password).option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
                # max_id = mssqlDF.agg({icol: "max"}).collect()[0][0]
                # mssqlDF = mssqlDF[mssqlDF[icol] <= max_id]
                if mssqlDF.count() > 0:
                    mssqlDF = mssqlDF.select(F.current_date().alias(
                        'offload_date'), '*', F.lit(database).alias('offload_database'))
                    mssqlDF.printSchema()
                    record_count = mssqlDF.count()
                    log("info", "Query read completed and loaded into spark dataframe")
                    log("info", "Starting load to datalake target path")
                    log("info", "Record count is "+str(record_count))
                    log("info", "Checking if table already exists")
                    current_count = 0
                    if spark._jsparkSession.catalog().tableExists(hive_db, table.partition(".")[2]):
                        log("info", "Table already exists")
                        log("Info", "Fetching the current count")
                        current_count = spark.sql(
                            "select count(*) from {}.{}".format(hive_db, table.partition(".")[2])).collect()[0][0]
                        log("info", "Current count: {}".format(current_count))
                    else:
                        log("info", "Table doesn't exists|| Creating now")
                    try:
                        target_path = "{}/{}/{}".format(target_path,
                                                        hive_db, table.partition(".")[2])
                        partition = "offload_database" if partition.lower() == "none" else partition
                        log("info", "partition : {}".format(str(partition)))
                        if op_format == "csv":
                            log("warn", "you need to be create csv table for {} with path {}".format(
                                table, target_path))
                        print(table,hive_db,op_format,op_mode,partition,target_path)
                        spark.sql(
                           "CREATE database IF NOT EXISTS {}".format(hive_db))
                        mssqlDF.write.saveAsTable(
                            hive_db+"."+table.partition(".")[2], format=op_format, mode=op_mode,  partitionBy=partition, path=target_path)
                        # s_file.write(str(date.today()) + " " + table + " " + str(current_count) + "\n")
                    except:
                        log("error", "Loading failed, Running for next table !!")
                    log("info", "dataframe loaded in {} format successfully into target path {}".format(
                        op_format, target_path))
                    log("info", "Data copyied for table {} successfully".format(table))
                    updated_count = spark.sql(
                        "select count(*) from {}.{}".format(hive_db, table.partition(".")[2])).collect()[0][0]
                    log("info", "Total record: {}, Source count: {} and Inserted Record: {}".format(
                        updated_count, record_count, updated_count-current_count))
                    # s_file.write(table + "\n")
                    s_file.write(str(date.today()) + " " + table + " " + str(record_count) + "\n")
                else:
                    log("info", "There are no new records to process!!")
            else:
                log("info", "Skipping as Already completed load for table:{}".format(table))
        sc.setLocalProperty("spark.scheduler.pool", None)
        # spark.stop()
     

if __name__ == '__main__':
    main()
