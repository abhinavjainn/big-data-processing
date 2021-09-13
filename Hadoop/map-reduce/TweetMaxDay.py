'''
Find out the the day with maximum number of tweets from the Rio Olymic Tweets
'''

from mrjob.job import MRJob
import time
from mrjob.step import MRStep

class Tweet(MRJob):

	# Steps
	def steps(self):
		return [MRStep(mapper=self.mapper, reducer=self.reducer),MRStep(reducer=self.reducer_find_max)]
		# return steps

	# Mapper 1
	def mapper(self, _, line):
		try:
			epoch_time_raw, tweet_id, tweet_text, device = line.split(";")
			epoch_time = int(epoch_time_raw)/1000
			day = time.strftime("%d-%m-%y",time.gmtime(epoch_time))

			yield day,1
		except:
			pass

    # Reducer 1
	def reducer(self, day, count):
		total = sum(count)
		yield None, (total, day)	

	# Reducer 2
	def reducer_find_max(self, key, values):
		yield max(values)

if __name__ == "__main__": 
	Tweet.run()