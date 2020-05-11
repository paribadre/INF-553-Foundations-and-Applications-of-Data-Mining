import os
import sys
from pyspark import SparkContext
from pyspark.sql import SQLContext
from graphframes import *

if __name__ == '__main__':

    # Accept input
    input_file_path = sys.argv[1]
    community_output_file_path = sys.argv[2]

    os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages graphframes:graphframes:0.6.0-spark2.3-s_2.11 pyspark-shell"
    sc = SparkContext("local[*]", "DMAssignment 4")

    sqlContext = SQLContext(sc)

    rdd = sc.textFile(input_file_path)
    rdd = rdd.map(lambda x: x.split(" "))

    # Create an RDD of edges
    node_edge_rdd = rdd.map(lambda s: (s[0], s[1]))
    node_edge_rdd_r = rdd.map(lambda s: (s[1], s[0]))
    node_edge_rdd_union = node_edge_rdd.union(node_edge_rdd_r)

    # Create an RDD of all vertices
    vertices_list = node_edge_rdd_union.collectAsMap().keys()
    vertices_rdd = sc.parallelize(vertices_list).map(lambda x: (x, ))

    # Create a vertices and edge data frames
    vertices_df = sqlContext.createDataFrame(vertices_rdd, ['id'])
    edge_df = sqlContext.createDataFrame(node_edge_rdd_union, ["src", "dst"])

    # Create graph and apply label propagation
    g = GraphFrame(vertices_df, edge_df)
    result = g.labelPropagation(maxIter=5)

    result = result.rdd.map(tuple)
    communities = result.map(lambda x: (x[1], x[0])).groupByKey().mapValues(lambda x: sorted(list(x))).map(lambda x: (len(x[1]), x[1]))\
    .sortBy(lambda x: (x[0], x[1][0])).map(lambda x: tuple(x[1])).collect()

    with open(community_output_file_path, "w") as file:
        for i in communities:
            file.write("\'{}\'\n".format("\', \'".join([str(j) for j in i]).strip(",")))



