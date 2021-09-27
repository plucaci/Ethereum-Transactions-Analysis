import functools
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col
from datetime import datetime, timedelta

sc = SparkContext()
spark = SparkSession(sc)


def timeline_drilldown(from_date, weeks):
    return datetime.strftime(datetime.strptime(from_date, "%Y-%m-%d") - timedelta(weeks=weeks), "%Y-%m-%d")


def timeline_drillup(from_date, weeks):
    return datetime.strftime(datetime.strptime(from_date, "%Y-%m-%d") + timedelta(weeks=weeks), "%Y-%m-%d")


def union_filtered_records(df):
    return functools.reduce(lambda before, after: before.union(after.select(after.columns)), df)


def fork_analysis(records, fork_timing, weeks):
    before_fork_timing = timeline_drilldown(fork_timing, weeks)
    before_fork_records = \
        records[(before_fork_timing < records['block_timestamp']) & (records['block_timestamp'] < fork_timing) &
                (records['value'] > 0)]
    before_fork_records = before_fork_records.withColumn('value', before_fork_records.value / (10 ** 18))
    before_fork_records = before_fork_records.persist()

    print("\n\n\nRecords " + str(weeks) + " weeks before fork timing " + fork_timing)
    before_fork_records.show(5, False)

    after_fork_timing = timeline_drillup(fork_timing, weeks)
    after_fork_records = \
        records[(fork_timing < records['block_timestamp']) & (records['block_timestamp'] < after_fork_timing) &
                (records['value'] > 0)]
    after_fork_records = after_fork_records.withColumn('value', after_fork_records.value / (10 ** 18))
    after_fork_records = after_fork_records.persist()

    print("\n\nRecords " + str(weeks) + " weeks after fork timing " + fork_timing)
    after_fork_records.show(5, False)

    analysis_results = union_filtered_records([before_fork_records, after_fork_records])
    analysis_results = analysis_results.sort('block_timestamp', ascending=True)
    analysis_results = analysis_results.persist()
    print("\n\n" + str(analysis_results.count()) + " records found " + str(weeks)
          + " weeks before and after fork timing " + fork_timing)
    analysis_results.show(10, False)

    return analysis_results


def get_top_fork_exploiters(df=None, top=10, weeks=2, fork_timing=None):

    if df is None:
        return None

    top_exploiters = df.sort('value', ascending=False)
    top_exploiters = top_exploiters.select(col('block_timestamp').cast("date"), 'from_address', 'to_address', 'value').\
        persist()
    print("\n\nTop" + str(top) + " \"Most Valuable Transactions\" made within " + str(weeks)
          + " weeks before and after fork timing " + fork_timing)
    top_exploiters.show(top, False)

    print("\n\nTop" + str(top) + " \"Most Valuable Transactions\" made within " + str(weeks)
          + " weeks before and after fork timing " + fork_timing
          + ", aggregated by date, senders and receivers")
    top_exploiters.groupby('block_timestamp', 'from_address', 'to_address').\
        sum().sort('sum(value)', ascending=False).show(top, False)

    print("\n\nTop" + str(top) + " \"Highest Sellers\" by total value of transactions made within " + str(weeks)
          + " weeks before and after fork timing " + fork_timing
          + ", aggregated by date and address")
    top_exploiters.groupby('block_timestamp', 'from_address')\
        .sum().sort('sum(value)', ascending=False).show(top, False)

    print("\n\nTop" + str(top) + " \"Highest Sellers\" by total value of transactions made within " + str(weeks)
          + " weeks before and after fork timing " + fork_timing
          + ", aggregated by address ONLY")
    top_exploiters.groupby('from_address')\
        .sum().sort('sum(value)', ascending=False).show(top, False)

    print("\n\nTop" + str(top) + " \"Highest Buyers\" by total value of transactions made within " + str(weeks)
          + " weeks before and after fork timing " + fork_timing
          + ", aggregated by date and address")
    top_exploiters.groupby('block_timestamp', 'to_address')\
        .sum().sort('sum(value)', ascending=False).show(top, False)

    print("\n\nTop" + str(top) + " \"Highest Buyers\" by total value of transactions made within " + str(weeks)
          + " weeks before and after fork timing " + fork_timing
          + ", aggregated by address ONLY")
    top_exploiters.groupby('to_address')\
        .sum().sort('sum(value)', ascending=False).show(top, False)


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


transactions = spark.read.csv('/data/ethereum/transactions/*.csv', header=True, inferSchema=True).persist()
transactions = transactions.withColumn('block_timestamp', transactions.block_timestamp.cast('timestamp')).persist()

constantin_fork_timing = "2019-02-28"

analysis = fork_analysis(transactions, constantin_fork_timing, 2)

fields_to_csv(df=analysis, fields=['block_timestamp', 'value'],
              timeline=True, timeline_col='block_timestamp',
              file="fork-analysis-results")

get_top_fork_exploiters(analysis, top=10, weeks=2, fork_timing=constantin_fork_timing)
