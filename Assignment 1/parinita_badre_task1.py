from os import environ
from pyspark import SparkContext, SparkConf
from operator import add
import time
import sys
import json

input_file_name = sys.argv[1]
output_file_name = sys.argv[2]
output = {}

conf = SparkConf().setAppName("DMTask1").setMaster('local[*]')
sc = SparkContext(conf=conf)

lines = sc.textFile(input_file_name).map(lambda x: json.loads(x))

totals = lines.map(lambda x: (1, x["review_count"])).reduce(lambda x, y: (x[0] + y[0], x[1] + y[1]))

count = totals[0]

total1 = totals[1]
avg = total1/count

df_pre = lines.map(lambda x: (x["name"], 1)).reduceByKey(lambda x, y: x + y)
df = df_pre.count()

yelping = lines.filter(lambda x: (x["yelping_since"][0:4] == '2011')).count()

pop = df_pre.sortBy(lambda x: x[1], False).take(10)
pop.sort(key=lambda x: (-x[1], x[0]))

top10s = lines.map(lambda x: (x["user_id"], x["review_count"])).sortBy(lambda x: x[1], False).take(10)
top10s.sort(key=lambda x: (-x[1], x[0]))

output['total_users'] = count
output['avg_reviews'] = avg
output['distinct_usernames'] = df
output['num_users'] = yelping
output['top10_popular_names'] = pop
output['top10_most_reviews'] = top10s

with open(output_file_name, 'w') as o:
    json.dump(output, o)

sc.stop()

