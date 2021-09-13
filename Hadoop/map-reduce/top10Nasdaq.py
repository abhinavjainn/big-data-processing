'''
Form the 40 year NASDAQ data on cluster (1970-2010), find out top 10 companies with highest 
amount exchanged in a single trading day. 
'''

from mrjob.job import MRJob

class Sort(MRJob):

    def mapper(self, _, line):
        try:
            fields = line.split(",")
            value = float(fields[6])*int(fields[7])
            publicKey = str(fields[1]+"-"+fields[2])
            yield (1,(value, publicKey))

        except :
            pass

    def reducer(self, value, publicKey):
        sorted_values = sorted(publicKey, reverse=True, key=lambda x: x[0])[:10]
        key = 0
        for i in sorted_values:
            key+=1
            yield(key,('{}-{}'.format(float(i[0]),str(i[1]).strip())))

if __name__ == '__main__':
    Sort.run()