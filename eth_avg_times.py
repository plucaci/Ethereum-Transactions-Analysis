from mrjob.job import MRJob
from datetime import datetime


class ETH_AvgTimes(MRJob):

    # Sorts the input to the reducers, only (and not the merged output too)
    MRJob.SORT_VALUES = True

    def mapper(self, _, line):
        fields = line.split(",")
        try:
            if len(fields) == 7:
                block_timestamp = int(fields[6])
                part_date = datetime.fromtimestamp(block_timestamp).strftime('%Y-%m')
                transaction_value = int(fields[3])

                yield part_date, (transaction_value, 1)
        except:
            pass

    def combiner(self, part_date, pairs):
        yield part_date, self.feature_counter(pairs)

    def reducer(self, part_date, pairs):
        (transaction_values, counts) = self.feature_counter(pairs)

        yield part_date, transaction_values/counts

    def feature_counter(self, pairs):
        (transaction_values, counts) = (0, 0)

        for (transaction_value, count) in pairs:
            transaction_values += transaction_value
            counts += count

        return transaction_values, counts


if __name__ == '__main__':

    # For more than 1 reducer, the merged output is NOT sorted, although complete
    ETH_AvgTimes.JOBCONF = {'mapreduce.job.reduces': '1'}
    ETH_AvgTimes.run()