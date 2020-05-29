import snap
import matplotlib.pyplot as plt

G1 = snap.LoadEdgeList(snap.PUNGraph, "/home/hieu/data/ttpt/project/facebook_large/musae_facebook_edges.csv", 0, 1, ',')

snap.DelSelfEdges(G1)

CmtyV = snap.TCnComV()
modularity = snap.CommunityCNM(G1, CmtyV)
count = 0
sizes = []
communities = []
for Cmty in CmtyV:
    listcmty = []
    for NI in Cmty:
        listcmty.append(NI)

    communities.append(listcmty)
    count += 1
    sizes.append(len(listcmty))
print("Number of communities:", count)
print("Largest community:", max(sizes))
print("Smallest community:", min(sizes))

# plot histogram of community sizes
sizes.sort()
plt.hist(sizes, log=True)
plt.xlabel("Size of community")
plt.ylabel("Number of communities")
plt.title("Sizes of CNM communities")
plt.show()
plt.savefig("cnm-sizes.png")
#print("Size of communities:", sizes)
print("The modularity of the network is %f" % modularity)
