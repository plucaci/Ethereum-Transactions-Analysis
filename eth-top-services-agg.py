from mrjob.job import MRJob
from mrjob.step import MRStep


class ETH_TopServices(MRJob):

    ####
    ###
    # JOB 1 - INITIAL AGGREGATION
    ###
    ####
    def transactions_filter(self, _, line):
        fields = line.split(",")

        try:
            if len(fields) == 7:
                yield fields[2], int(fields[3])
        except:
            pass

    def transactions_agg_combiner(self, to_address, value):
        yield to_address, sum(value)

    def transactions_agg_reducer(self, to_address, value):
        yield to_address, sum(value)

    def steps(self):
        return [MRStep(mapper   = self.transactions_filter,
                       combiner = self.transactions_agg_combiner,
                       reducer  = self.transactions_agg_reducer)]

if __name__ == '__main__':
    ETH_TopServices.JOBCONF = {'mapreduce.job.reduces': '35'}
    ETH_TopServices.run()