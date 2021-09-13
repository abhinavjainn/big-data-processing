'''
Form the 40 year NASDAQ data on cluster (1970-2010), find out the year in which 
Technology sector had the highest movement. 
Uses inner join with companylist.tsv file to find out the sector of a company.
'''

from mrjob.job import MRJob
from mrjob.step import MRStep

class NsdaqTopTechYear(MRJob):

	sector_table = {}

	def steps(self):
		return [MRStep(mapper_init=self.mapper_join_init, mapper=self.mapper_repl_join, reducer=self.reducer)]
				# MRStep(mapper=self.mapper_length,reducer=self.reducer_sum)]

	def mapper_join_init(self):
		with open("companylist.tsv") as f:
			for line in f:
				symbol, name, ipoYear, sector, industry = line.split("\t")
				self.sector_table[symbol]=sector

	def	mapper_repl_join(self,_,line):
		try:
			fields = line.split(",")
			symbol = fields[1]
			year = fields[2][:4]
			volume = fields[7]
			sector = self.sector_table[symbol]
			if sector == 'Technology':
				# yield (year, 1)
				yield year, int(volume)
		except:
			pass

	def reducer(self,year,count):
		yield year, sum(count)

if __name__ == '__main__':
    NsdaqTopTechYear.run()