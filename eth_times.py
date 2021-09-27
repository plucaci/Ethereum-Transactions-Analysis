from mrjob.job import MRJob
from datetime import datetime


class ETH_Times(MRJob):

    # Sorts the input to the reducers, only (and not the merged output too)
    MRJob.SORT_VALUES = True

    def mapper(self, _, line):
        fields = line.split(",")

        try:
            if len(fields) == 7:
                block_timestamp = int(fields[6])
                part_date = datetime.fromtimestamp(block_timestamp).strftime('%Y-%m')

                yield part_date, 1
        except:
            pass

    def combiner(self, part_date, counts):
        yield part_date, sum(counts)

    def reducer(self, part_date, counts):
        yield part_date, sum(counts)


if __name__ == '__main__':

    # For more than 1 reducer, the merged output is NOT sorted, although complete
    ETH_Times.JOBCONF = {'mapreduce.job.reduces': '1'}
    ETH_Times.run()










"""import pyspark
from pyspark.sql import SparkSession as spark

sc = pyspark.SparkContext()

def collect_transactions():

    df = sc.read.options(delimiter=',', header='True').csv("/data/ethereum/transactionSmall/")

    df.printSchema()
    df.take(2)

    return df

def get_avg_monthly_transactions(df):
    return df.select("block_timestamp").rdd.reduceByKey(lambda x, y: x + y).collect()

df = collect_transactions()

print(get_avg_monthly_transactions(df))"""