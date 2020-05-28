# add GraphFrames package to spark-submit
from pyspark.sql import SQLContext, SparkSession
from pyspark import SparkContext
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages graphframes:graphframes:0.8.0-spark3.0-s_2.12 pyspark-shell'
import pyspark
# create SparkContext and Spark Session
sc = pyspark.SparkContext("local[*]")
spark = SparkSession.builder.appName('Graphframe').getOrCreate()
# import GraphFrames
from graphframes import *
import networkx as nx
import matplotlib.pyplot as plt 

vertices = spark.read.csv("/home/hieu/data/ttpt/project/facebook_large/musae_facebook_target.csv", header = True)
edges_df = spark.read.csv("/home/hieu/data/ttpt/project/facebook_large/musae_facebook_edges.csv", header = True)
edges = edges_df.withColumnRenamed("id_2", "dst").withColumnRenamed("id_1", "src")
# edges.show(10)

def PlotGraph(edge_list):
    Gplot=nx.Graph()
    for row in edge_list.select('src','dst').take(4000):
        Gplot.add_edge(row['src'],row['dst'])

    options = {
    'node_color': 'black',
    'node_size': 10,
    'line_color': 'grey',
    'linewidths': 0,
    'width': 0.1,
}

    plt.subplot(121)
    nx.draw(Gplot, **options)
    plt.show()

if __name__ == "__main__":
    g = GraphFrame(vertices, edges)
    edges_sample = g.edges.sample(True, 10000/172000)
    edges_sample.show(10)
    PlotGraph(edges_sample)
  