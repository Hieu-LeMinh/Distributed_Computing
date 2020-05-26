from pyspark import SparkContext
from pyspark.sql import SQLContext, SparkSession
import pyspark.sql.functions as f

# "0": [
        3133,
        3825,
        236,
        874,
        1072,
        143,
        1078,
        901
# id,facebook_id,page_name,page_type

spark = SparkSession.builder.master("local").appName("Test").getOrCreate()
df = spark.read.csv("/home/hieu/data/ttpt/project/facebook_large/musae_facebook_target.csv", header = True)

def get_num_pagetype(df):
    result = df.groupBy("page_type").count().select("page_type", f.col('count').alias('number_nodes'))
    result.show()
    return result

def get_per_pagetype(df):
    sum = df.groupBy().sum().collect()[0][0]
    result = df.withColumn('percent', f.col('number_nodes')/sum)
    result.show()
    return result

if __name__ == "__main__":
    file = "/home/hieu/data/ttpt/project/facebook_large/musae_facebook_target.csv"
    # df = get_df(file)
    num_node_page_type = get_num_pagetype(df)
    percent_pagetype = get_per_pagetype(num_node_page_type)