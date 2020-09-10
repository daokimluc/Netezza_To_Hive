import os,sys
import pyspark
from multiprocessing import Pool
from pyspark.sql import SparkSession
import time
def train(db):

    print(db)
    spark = SparkSession \
        .builder \
        .appName("scene_"+str(db)) \
        .config("spark.executor.cores", '1') \
        .getOrCreate()
    print(spark.createDataFrame([[1.0],[2.0]],['test_column']).collect())

if __name__ == '__main__':
    p = Pool(10)
    for db in range(10):
        p.apply_async(train,args=(db,))    
    p.close()
    p.join()
    #train(4)
