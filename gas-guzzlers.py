import time
import functools
from datetime import datetime, timedelta
from pyspark import SparkContext
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *
from pyspark.sql import SparkSession

sc = SparkContext()
spark = SparkSession(sc)


def data_import(file = ""):
    if file == "":
        return None

    df = spark.read.csv(file, sep=r'\t', inferSchema=True, header=False)
    df = df.withColumnRenamed(existing=df.columns[0], new='contract_addr')
    df = df.withColumnRenamed(existing=df.columns[1], new='total_value')
    df = df.withColumn('total_value', df.total_value / (10 ** 18))


    print("\n\n\nTop10 Contracts Dataset Preview:")
    df.show(n=20, truncate=False)

    return df


def joiner(df1=None, df2=None):

    if (df1 is None) or (df2 is None):
        return None

    keys = [df1.to_address == df2.contract_addr]
    df = df1.join(df2, on=keys, how='inner')
    df = df.select(df.columns[4:])
    df = df.select('contract_addr', 'total_value', 'gas', 'gas_price', 'block_timestamp')
    df = df.withColumn('gas_price', df.gas_price / (10 ** 18))

    print("\n\n\nTransactions Joined Dataset Preview:")
    df.show(n=20, truncate=False)

    return df


def fields_to_csv(df, fields=None, timeline=False, timeline_col=None, file="output"):

    if fields is None:
        return None

    sel = df.select(fields)

    if timeline is True:
        if timeline_col is None:
            return None
        sel = sel.sort(timeline_col, ascending=True)

    sel = sel.persist()

    print("\n\n\nSaved Dataset Preview:")
    sel.show(n=20, truncate=False)

    sel.write.save(file + ".csv", format="csv")


contracts = data_import(file="./output-services-top10.tsv")
inmem_c = contracts.persist()

if contracts is None:
    exit(-1)

transactions = spark.read.csv('/data/ethereum/transactions/*.csv', header=True, inferSchema=True)
inmem_t = transactions.persist()
inmem_t = inmem_t.withColumn('block_timestamp', inmem_t.block_timestamp.cast('timestamp')).persist()

analysis = joiner(inmem_t, inmem_c)
analysis = analysis.persist()

fields_to_csv(df=analysis, fields=['block_timestamp', 'total_value', 'gas', 'gas_price'],
              timeline=True, timeline_col='block_timestamp',
              file="gas-analysis")
