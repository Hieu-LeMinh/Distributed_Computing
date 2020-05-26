from pyspark import *
from graphframes import *

spark = SparkSession.builder.appName("Test2").getOrCreate()
nodes_df = spark.read.csv("/home/hieu/data/ttpt/project/facebook_large/musae_facebook_target.csv", header = True)
edges_df = spark.read.csv("/home/hieu/data/ttpt/project/facebook_large/musae_facebook_edges.csv", header = True)

def ini_graph(nodes_df, edges_df):
    return GraphFrame(nodes_df, edges_df)

if __name__ == "__main__":
    g = ini_graph(nodes_df, edges_df)
    g.edges.show()
