from mrjob.job import MRJob
from mrjob.step import MRStep


class ETH_TopServices(MRJob):

    ####
    ###
    # JOB 2 - JOINING TRANSACTIONS/CONTRACTS AND FILTERING
    ###
    ####
    def repartition_filter(self, _, line):
        try:
            if len(line.split('\t')) == 2:
                fields = line.split('\t')

                join_key = fields[0].strip('"')
                join_value = int(fields[1])
                yield join_key, join_value

            elif len(line.split(',')) == 5:
                fields = line.split(',')

                join_key = fields[0]

                ## See comments in reducer for the meaning of the "contract" string
                yield join_key, "contract"
        except:
            pass

    def repartition_reducer(self, contract_addr, joined_values):


        ###
        ## If the aggregated values for a given address contains the "contract" string,
        ##  then the address exists within the 'contracts' dataset.
        ## Hence, the address is that of a contract.
        ###
        ## There could be multiple "contract" strings for a given address,
        ##  if that given address appears multiple times within the 'contracts' dataset.
        ###

        contract = False
        contract_value = None

        for jv in joined_values:

            ###
            ## Extracting only the aggregated value (numeric) of the contract, if exists
            ###
            if jv != "contract":
                contract_value = jv
            else:
                contract = True

        if (contract_value is not None) and (contract is True):
            yield "top10", (contract_addr, contract_value)

    ####
    ###
    # JOB 3 - TOP TEN
    ###
    ####
    def get_top10_services(self, _, contract_data):
        top10 = sorted(contract_data, reverse=True, key=lambda tup: tup[1])
        top10 = top10[:10]

        for record in top10:
            yield record[0], record[1]

    def steps(self):
        return [MRStep(mapper  = self.repartition_filter, reducer = self.repartition_reducer),
                MRStep(reducer = self.get_top10_services)]


if __name__ == '__main__':
    ETH_TopServices.JOBCONF = {'mapreduce.job.reduces': '35'}
    ETH_TopServices.run()
