from mrjob.job import MRJob
from mrjob.step import MRStep
import json as jsn


class ETH_LucativeScams(MRJob):

    scams = {}

    def addr_filter(self):

        with open("scams.json") as f:
            scams_data = jsn.load(f)

            for k, v in scams_data['result'].items():
                self.scams[k] = {
                    'category': v.get('category'),
                    'subcategory': v.get('subcategory'),
                    'url': v.get('url'),
                    'status': v.get('status')
                }

    def scam_addr_mapper(self, _, line):
        fields = line.split(",")
        try:
            if len(fields) == 7:
                scam_addr = fields[2]
                transaction_value = int(fields[3])

                if scam_addr in self.scams:
                    yield scam_addr, transaction_value
        except:
            pass

    def scam_addr_combiner(self, scam, values):
        yield scam, sum(values)

    def scam_addr_reducer(self, scam, values):
        yield "lucrative", (scam, sum(values))

    def get_lucrative_scam(self, _, scam_data):
        most_lucrative_scam = max(scam_data, key=lambda x: x[1])

        scam_addr = str(most_lucrative_scam[0])
        scam_total = most_lucrative_scam[1]
        scam_category = self.scams.get(scam_addr).get('category')
        scam_subcategory = self.scams.get(scam_addr).get('subcategory')
        scam_url = self.scams.get(scam_addr).get('url')
        scam_status = self.scams.get(scam_addr).get('status')

        result  = f"Most lucrative scam is {scam_addr}" + "\n"
        result += f"Totalling {scam_total} in sent Wei" + "\n"
        result += f"Belonging to \"{scam_category}\" category, and subcategory \"{scam_subcategory}\"" + "\n"
        result += f"With URL: \"{scam_url}\"" + "\n"
        result += f"Being \"{scam_status}\" when the dataset has been compiled" + "\n"

        print(result)

        yield "JOB", "SUCCESSFUL"

    def steps(self):
        return [MRStep(mapper_init=self.addr_filter,
                       mapper=self.scam_addr_mapper, combiner=self.scam_addr_combiner, reducer=self.scam_addr_reducer),
                MRStep(reducer_init = self.addr_filter, reducer=self.get_lucrative_scam)]


if __name__ == '__main__':
    ETH_LucativeScams.run()
