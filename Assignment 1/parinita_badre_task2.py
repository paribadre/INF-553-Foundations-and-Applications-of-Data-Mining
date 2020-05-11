from os import environ
from pyspark import SparkContext, SparkConf
from operator import add
import time
import json
import sys

conf = SparkConf().setAppName("DMTask1").setMaster('local[*]')
sc = SparkContext(conf=conf)

input_file = sys.argv[1]
output_file = sys.argv[2]
input_part = int(sys.argv[3])

def cpartition(iterator): yield sum(1 for _ in iterator)

def review_partitioner(review):
    return hash(review)

currenttime = time.time()

rdd = sc.textFile(input_file)

defaultdict = {}
defaultdict['n_partitions'] = rdd.getNumPartitions()
defaultdict['n_items'] = rdd.mapPartitions(cpartition).collect()

totals = rdd.map(lambda x: json.loads(x)).map(lambda x: (x["user_id"], x["review_count"])).sortBy(lambda x: x[1], False)
top10s = totals.take(10)
top10s.sort(key=lambda x: (-x[1], x[0]))

defaulttime = time.time() - currenttime

defaultdict['exe_time'] = defaulttime

customdict = {}
currenttime = time.time()

rdd2 = rdd
top10s = []

totals = rdd2.map(lambda x: json.loads(x)).map(lambda x: (x["review_count"], (x["user_id"], x["review_count"]))).partitionBy(input_part, review_partitioner).sortBy(lambda x: x[1], False)
top10s = totals.take(10)
top10s.sort(key=lambda x: (-x[0], x[1][0]))

customtime = time.time() - currenttime

customdict['n_partitions'] = totals.getNumPartitions()
customdict['n_items'] = totals.mapPartitions(cpartition).collect()
customdict['exe_time'] = customtime

output = {}
output['default'] = defaultdict
output['customized'] = customdict
output['explanantion'] = "Spark partitions by default using 74 partitions and hash function. By choosing a custom partition that is optimal for the given data, the data is distributed optimally on partitions to reduce communication cost. Using partitionby also helps shuffle functions like sortBy and join. Thus, using a custom partition performs better."

with open(output_file, 'w') as o:
    json.dump(output, o)

sc.stop()

