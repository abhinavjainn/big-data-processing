'''
Using Rio Olympics dataset on cluster, find the average length of tweets and 
average number of hastags per tweet.
'''

from mrjob.job import MRJob
import time
from mrjob.step import MRStep
import statistics

class TweetStats(MRJob):

	def steps(self):
		return [MRStep(mapper=self.mapper, reducer=self.reducer)]
		
	def mapper(self, _, line):
		try:
			epoch_time_raw, tweet_id, tweet_text, device = line.split(";")
			yield "Avg_Tweet_Length", len(tweet_text)
			yield "Avg_Hash_Count", tweet_text.count("#")
		except:
			pass

	def reducer(self, key, values):
		avg = statistics.mean(values)
		yield key, (avg)	

if __name__ == "__main__": 
	TweetStats.run()