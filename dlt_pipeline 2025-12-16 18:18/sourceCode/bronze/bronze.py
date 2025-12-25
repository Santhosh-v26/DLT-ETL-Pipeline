import dlt 
from datetime import datetime
from pyspark.sql.functions import *
import os

def lastest_file(folder_path):
    files = [f.path for f in dbutils.fs.ls(folder_path) if f.path.endswith('csv')]
    return max(files)

@dlt.table(
    name = 'main.dlt_bronze.customers',
    comment = 'ingest data from uc volume'
)

def ingest_customers():
    path = '/Volumes/main/volume/task/first_dlt/customers/'
    return(
        spark.read.format('csv')\
        .option('header',True)\
        .option('inferSchema',True)\
        .load(path)\
        .withColumn("ingest_time", current_timestamp())
    )

@dlt.table(
    name = 'main.dlt_bronze.products',
    comment = 'ingest data from uc volume'
)

def ingest_products():
    path = '/Volumes/main/volume/task/first_dlt/products/'
    return(
        spark.read.format('csv')\
        .option('header',True)\
        .option('inferSchema',True)\
        .load(path)\
        .withColumn("ingest_time", current_timestamp())
    )


@dlt.table(
    name = 'main.dlt_bronze.categories',
    comment = 'ingest data from uc volume'
)

def ingest_categories():
    path = '/Volumes/main/volume/task/first_dlt/categories/'
    return(
        spark.read.format('csv')\
        .option('header',True)\
        .option('inferSchema',True)\
        .load(path)\
        .withColumn("ingest_time", current_timestamp())
    )


@dlt.table(
    name = 'main.dlt_bronze.orders',
    comment = 'ingest data from uc volume'
)

def ingest_orders():
    return(
        spark.readStream.format('cloudFiles')\
        .option('cloudFiles.format','csv')\
        .option('cloudFiles.inferColumnTypes',True)\
        .option('header',True)\
        .load('/Volumes/main/volume/task/first_dlt/orders/')\
        .withColumn("ingest_time", current_timestamp())\
        .withColumn("source_file", col("_metadata.file_path"))
    )

@dlt.table(
    name = 'main.dlt_bronze.payments',
    comment = 'ingest data from uc volume'
)

def ingest_payments():
    return(
        spark.readStream.format('cloudFiles')\
        .option('cloudFiles.format','csv')\
        .option('cloudFiles.inferColumnTypes',True)\
        .option('header',True)\
        .load('/Volumes/main/volume/task/first_dlt/payments/')\
        .withColumn("ingest_time", current_timestamp())\
        .withColumn("source_file", col("_metadata.file_path"))
    )

