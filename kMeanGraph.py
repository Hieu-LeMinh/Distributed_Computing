import snap
import numpy as np

from sklearn.decomposition import TruncatedSVD
from pyspark.ml.clustering import KMeans


def compute_adjacency_matrix(data="/home/hieu/data/ttpt/project/facebook_large/musae_facebook_edges.csv"):

    G1 = snap.LoadEdgeList(snap.PUNGraph, data, 0, 1, ',')

    n = G1.GetNodes()
    A = np.zeros(shape=(n,n))

    for NI in G1.Nodes():
        for Id in NI.GetOutEdges():
            A[NI.GetId()][Id] = 1
    return A

def dimentions_reducer(data_matix, dim):
    svd = TruncatedSVD(n_components=dim, random_state=42)

    svd.fit(data_matix)
    return svd

def k_mean(data, num_clus):
    kmeans = KMeans().setK(num_clus).setSeed(42)
    model = kmeans.fit(data)

    predictions = model.transform(data)
    return predictions

if __name__ == '__main__':
    data_path = ''
    A = compute_adjacency_matrix(data_path)
    D = np.diag(A.sum(axis=1))
    L = D - A

    reducer = dimentions_reducer(L, dim=1000)
    comp_L = reducer.transform(L)
    predict = k_mean(comp_L, num_clus=5)





