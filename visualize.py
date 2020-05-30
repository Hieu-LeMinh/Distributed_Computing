import snap
import numpy as np

def GraphVisualization(G1):
	labels = snap.TIntStrH()
	# page_name = snap.TIntStrH()
	# page_type = snap.TIntStrH()

	for NI in G1.Nodes():
	    labels[NI.GetId()] = str(NI.GetId())
	    # page_name[NI.GetId()] = str(targets['page_name'][NI.GetId()])
	    # page_type[NI.GetId()] = str(targets['page_type'][NI.GetId()])

	snap.DrawGViz(G1, snap.gvlDot, "output.png", " ", labels)

def PlotVisual(G1):
	snap.PlotInDegDistr(G1, "Facebook", "Facebook In Degree")
	snap.PlotWccDistr(G1, "Facebook", "Facebook - wcc distributaion")
	snap.PlotSccDistr(G1, "Facebook", "Facebook - scc distributaion")
	snap.PlotClustCf(G1, "Facebook", "Facebook - clustering coefficient")
	snap.PlotKCoreNodes(G1, "Facebook", "Facebook - k-core nodes")
	snap.PlotKCoreEdges(G1, "Facebook", "Facebook - k-core edges")

if __name__ == "__main__":
	G1 = snap.LoadEdgeList(snap.PUNGraph, "/home/hieu/data/ttpt/project/facebook_large/musae_facebook_edges.csv", 0, 1, ',')

	PlotVisual(G1)
	# GraphVisualization(G1)
