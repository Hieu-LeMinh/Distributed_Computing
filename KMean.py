import pandas as pd
import json
import snap
import numpy as np
import snap
from sklearn.decomposition import TruncatedSVD
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator



class KMean():
    def __init__(self):
        self.sc = SparkContext('local')
        self.spark = SparkSession(self.sc)
    
    def covert_to_df(self, arr):
        df = pd.DataFrame(arr)
        df = self.spark.createDataFrame(df)
        assembler = VectorAssembler( inputCols=[x for x in df.columns], outputCol='features')
        new_df = assembler.transform(df)
        data = new_df.select("features")
        return data

    def kmean_spark(self, arr, num_clus):
        df = pd.DataFrame(arr)
        df = self.spark.createDataFrame(df)

        assembler = VectorAssembler( inputCols=[x for x in df.columns], outputCol='features')
        new_df = assembler.transform(df)
        data = new_df.select("features")
        kmeans = KMeans(k=num_clus, seed=1)
        model = kmeans.fit(data)
        return model

    def kmean_sk(self):
        pass

    def dimentions_reducer(self, data_matix, dim=500):
        svd = TruncatedSVD(n_components=dim, random_state=42)

        svd.fit(data_matix)
        return svd

    def load_node_features(self, data_path='./dataset/musae_facebook_features.json'):
        print("Loading data...")
        with open(data_path) as f:
            data = json.load(f)

        featureMatrix = np.zeros((22470,4714), dtype=np.int8)
        for node_id in range(22470):
            for feature in range(4714):
                if feature in data[str(node_id)]:
                    featureMatrix[node_id, feature] = 1

        return featureMatrix
    
    def compute_L_matrix(self, data='./dataset/musae_facebook_edges.csv'):
        G1 = snap.LoadEdgeList(snap.PUNGraph, data, 0, 1, ',')

        n = G1.GetNodes()
        A = np.zeros(shape=(n,n))

        for NI in G1.Nodes():
            for Id in NI.GetOutEdges():
                A[NI.GetId()][Id] = 1
        D = np.diag(A.sum(axis=1))
        L = D - A
        return L
    
    def get_clus_center(self, model):
        centers = model.clusterCenters()
        return centers
    
    def silhouette_eval(self, prediction):
        evaluator = ClusteringEvaluator()
        silhouette = evaluator.evaluate(prediction)
        return silhouette

if __name__=='__main__':
    k_mean = KMean()
    feature_matrix = k_mean.load_node_features()
    reducer = k_mean.dimentions_reducer(feature_matrix, dim=500)
    comp_data = reducer.transform(feature_matrix)
    print(type(comp_data))
    print(comp_data)
    model = k_mean.kmean_spark(comp_data, num_clus=4)
    center = k_mean.get_clus_center(model)
    print(center)
    print('*'*10)

    data = k_mean.covert_to_df(comp_data)
    predict = model.transform(data)
    score = k_mean.silhouette_eval(predict)
    print(score)
    # L = k_mean.compute_L_matrix()
    # reducer = k_mean.dimentions_reducer(L, dim=500)
    # comp_data = reducer.transform(L)
    # print(L)

