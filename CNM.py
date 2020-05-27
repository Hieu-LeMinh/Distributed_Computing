import snap

def CNMGraph(Graph):
	CmtyV = snap.TCnComV()
	modularity = snap.CommunityCNM(Graph, CmtyV)
	for Cmty in CmtyV:
	    print("Community: ")
	    for NI in Cmty:
	        print(NI)
	print("The modularity of the network is %f" % modularity)

if __name__ == '__main__':
	G1 = snap.LoadEdgeList(snap.PUNGraph, "musae_facebook_edges.csv", 0, 1, ',')

	CNMGraph(G1)