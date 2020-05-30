from pyspark import SparkContext
from pyspark.sql import SQLContext, SparkSession
import pyspark.sql.functions as f

spark = SparkSession.builder.master("local").appName("Test").getOrCreate()
edges = spark.read.csv("/home/hieu/data/ttpt/project/facebook_large/musae_facebook_edges.csv", header = True)
nodes = spark.read.csv("/home/hieu/data/ttpt/project/facebook_large/musae_facebook_target.csv", header = True)

def get_num_pagetype(nodes):
    result = nodes.groupBy("page_type").count().select("page_type", f.col('count').alias('number_nodes'))
    result.show()
    return result

def get_per_pagetype(nodes):
    sum = nodes.groupBy().sum().collect()[0][0]
    result = nodes.withColumn('percent', f.col('number_nodes')/sum)
    print('Ti le cac loai Page: ')
    result.show()
    return result

def filter_pagename(nodes, pagename):
    result = nodes.filter(nodes.page_name == pagename)
    print('Tim Kiem thong tin Page: ', pagename)
    result.show()
    return result

def page_like_page(nodes, edges, pagename):
    page = nodes.filter(nodes.page_name == pagename)
    page_like_1 = page.join(edges, page.id == edges.id_1).select('id_2')
    page_like_1 = page_like_1.join(nodes, page_like_1.id_2 == nodes.id).select('id', 'page_name', 'page_type')
    page_like_2 = page.join(edges, page.id == edges.id_2).select('id_1')
    page_like_2 = page_like_2.join(nodes, page_like_2.id_1 == nodes.id).select('id', 'page_name', 'page_type')
    result = page_like_1.union(page_like_2)
    result.show(10)
    print("Cac Page lien ket voi Page", pagename)
    return result


if __name__ == "__main__":

    num_node_page_type = get_num_pagetype(nodes)

    percent_pagetype = get_per_pagetype(num_node_page_type)
    
    filter_pagename(nodes, pagename='The Voice of China 中国好声音')

    page_like_page(nodes, edges, pagename= 'Policía de Seguridad Aeroportuaria')