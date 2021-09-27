import time
import functools
from datetime import datetime, timedelta
from pyspark import SparkContext
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from operator import add

sc = SparkContext()
spark = SparkSession(sc)


def is_valid_transaction(line):
    fields = line.split(",")

    try:
        if len(fields) != 7:
            return False

        int(fields[3])
        return True

    except:
        return False


def is_valid_contract(line):
    return len(line.split(",")) == 5


def addr_value(line):
    fields = line.split(",")

    addr  = fields[2]
    value = int(fields[3])

    return addr, value


def contractAddr(line):
    addr = line.split(",")[0]

    return addr, None


transactions = sc.textFile('/data/ethereum/transactions/*.csv')
transactions = transactions.filter(is_valid_transaction).persist()

transactions_agg = transactions.map(addr_value).reduceByKey(add).persist()


contracts = sc.textFile('/data/ethereum/contracts/*')
contracts = contracts.filter(is_valid_contract).persist()

contracts_addr = contracts.map(contractAddr).persist()


joined_tc = transactions_agg.join(contracts_addr).persist()

# print(joined_tc.take(3))
# tup[0]                                          tup[1][0]
# (u'0x5d16d41d6c3466ca319b02ca5d1575dc1497c37b', (4416400000000000000, None)),"

top10 = joined_tc.takeOrdered(10, key=lambda tup: -tup[1][0])

print("Contract Address\t\t\t\tContract Value (in Wei)")
print("----------------\t\t\t\t--------------------------")
for record in top10:
    print(str(record[0]) + "\t" + str(record[1][0]))
