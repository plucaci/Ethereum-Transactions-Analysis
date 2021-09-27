from mrjob.job import MRJob
from mrjob.step import MRStep


class ETH_PopularScams(MRJob):

    def scam_category_mapper(self, _, line):
        fields = line.split(",")
        try:
            if len(fields) == 3:
                category = fields[1]

                yield category, 1
        except:
            pass

    def scam_category_combiner(self, category, count):
        yield category, sum(count)

    def scam_category_reducer(self, category, count):
        yield "popular", (category, sum(count))

    def list_sort_categories(self, _, pairs):
        sorted_categories = sorted(pairs, reverse=True, key=lambda x: x[1])

        yield f"Most popular type of scam, with {sorted_categories[0][1]} counts: ", sorted_categories[0][0]

        for tup in sorted_categories:
            yield tup[0], tup[1]

    def steps(self):
        return [MRStep(mapper=self.scam_category_mapper, combiner=self.scam_category_combiner,
                       reducer=self.scam_category_reducer),
                MRStep(reducer=self.list_sort_categories)]


if __name__ == '__main__':
    ETH_PopularScams.run()
