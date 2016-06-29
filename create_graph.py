import sys
import networkx as nx
import matplotlib.pyplot as plt
d = {}
partition = []
for x in range(1, len(sys.argv)-2):
    partition.append(int(sys.argv[x]))
in_d = float(sys.argv[len(sys.argv)-2])
out_d = float(sys.argv[len(sys.argv)-1])
G = nx.random_partition_graph(partition, in_d, out_d)
nx.draw(G)
plt.show()
for x in G.edges():
    key, value = x
    k, v = int(key), int(value)
    if k in d:
        d[k].append(v)
    else:
        d[k] = []
        d[k].append(v)
line = ""
for p in G.nodes():
    p = int(p)
    if p not in d:
        d[p] = []
s = "Graph_" + str(sum(partition)) + "_" + str(int(in_d*int((10**(len(sys.argv[len(sys.argv)-2])-1)))))+"_"+ str(int(out_d*int((10**(len(sys.argv[len(sys.argv)-1])-1)))))+".txt"
with open(s, "w") as output:
    for key, values in d.items():
        line = "[" + str(key) + ",0,["
        if values != []:
            for x in values:
                line = line + "[" + str(x) + ",0],"
            line = line[:-1] + "]]"
        else:
            line = line + "]]"
        print(line, file=output)
