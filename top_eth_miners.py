from mrjob.job import MRJob
from mrjob.step import MRStep


class ETH_TopMiners(MRJob):

    def input_filter(self, _, line):
        fields = line.split(",")

        try:
            if len(fields) == 9:
                yield fields[2], int(fields[4])
        except:
            pass

    def agg_combiner(self, miner, block_size):
        yield miner, sum(block_size)

    def agg_reducer(self, miner, block_size):
        yield "aggregated", (miner, sum(block_size))

    def sorting_reducer(self, _, pairs):
        sorted_values = sorted(pairs, reverse=True, key=lambda tup: tup[1])
        i = 0
        for value in sorted_values:
            yield (value[0], value[1])

            i += 1
            if i >= 10:
                break

    def steps(self):
        return [MRStep(mapper=self.input_filter, combiner=self.agg_combiner, reducer=self.agg_reducer),
                MRStep(reducer=self.sorting_reducer)]

if __name__ == '__main__':
    ETH_TopMiners.run()
