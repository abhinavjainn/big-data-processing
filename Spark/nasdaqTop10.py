
import pyspark
import re
sc = pyspark.SparkContext()

#we will use this function later in our filter transformation
def is_good_line(line): try:
    fields = line.split(’,’) if len(fields) != 9:
               return False
           float(fields[6])
           float(fields[7])
           return True
        except:
    return False

lines = sc.textFile("/data/NASDAQ")
clean_lines = lines.filter(is_good_line)
features=clean_lines.map(lambda l: (l.split(',')[1],l.split(',')[2],float(l.split(',')[6])*float(l.split(',')[7])))
top10 = features.takeOrdered(10, key = lambda x: -x[2]) 

for record in top10:
    print("{}: {};{}".format(record[0],record[1],record[2]))