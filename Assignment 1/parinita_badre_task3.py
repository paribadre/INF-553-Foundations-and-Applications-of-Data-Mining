from os import environ
from pyspark import SparkContext, SparkConf
from operator import add
import time
import json
import sys

conf = SparkConf().setAppName("DMTask1").setMaster('local[*]')
sc = SparkContext(conf=conf)

input_file_name1 = sys.argv[1]
input_file_name2 = sys.argv[2]
output_file_name1 = sys.argv[3]
output_file_name2 = sys.argv[4]

reviewdata = sc.textFile(input_file_name1)

businessdata = sc.textFile(input_file_name2)

business_reviewSorted = []

business = businessdata.map(lambda x: json.loads(x)).map(lambda x: (x["business_id"], x["state"]))
review = reviewdata.map(lambda x: json.loads(x)).map(lambda x: (x["business_id"], x["stars"]))

business_review = business.join(review)
business_review = business_review.map(lambda x: (x[1][0], x[1][1])).groupByKey().map(lambda x: (x[0], sum(x[1]) / len(x[1])))
business_reviewSorted = business_review.sortBy(lambda x: (-x[1], x[0]))


with open (output_file_name1, "w") as o:
    o.write("state,stars\n")
    for i in business_reviewSorted.collect():
        o.write(i[0]+","+str(i[1])+"\n")

startcollect = time.time()

business_reviewSortedcollect = business_reviewSorted.collect()
business_reviewSortedcollectTop5=business_reviewSortedcollect[:5]
print(business_reviewSortedcollectTop5)

Collecttime = time.time() - startcollect

starttake = time.time()

business_reviewSortedtake = business_reviewSorted.take(5)
print(business_reviewSortedtake)

Taketime = time.time() - starttake

output_dict = {}
output_dict['m1'] = Collecttime
output_dict['m2'] = Taketime
output_dict['explanation'] = "Collect function takes more time because it collects all the data from the RDD, whereas Take function only takes the required no. of elements specified in the parameter. Therefore, take function is faster."

with open(output_file_name2, "w") as o:
    json.dump(output_dict, o)

sc.stop()
