from mrjob.job import MRJob
from mrjob.step import MRStep
import statistics

class Tweet50(MRJob):

    def steps(self):
        return [MRStep(mapper=self.mapper, reducer=self.reducer)]
        # return [MRStep(mapper=self.mapper, combiner = self.combiner, reducer=self.reducer)]

    def mapper(self, _, line):
        fields = line.split(";;;")
        mbtiType = fields[1]
        messages = fields[2:]
        tot_message_len = 0
        for item in messages:
            tot_message_len += len(item)

        yield mbtiType, tot_message_len / 50  #Since 50 count if fixed

    # def combiner(self,mbtiType, avg_message_len):
    #     yield  mbtiType, statistics.mean(avg_message_len)

    def reducer(self,mbtiType, avg_message_len):
        yield  mbtiType, statistics.mean(avg_message_len)

if __name__ == '__main__':
    Tweet50.run()