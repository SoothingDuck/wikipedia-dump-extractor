# %% Modules
import igraph as ig
import matplotlib.pyplot as plt
import pandas as pd
from iterfzf import iterfzf

# %% Read data
g = ig.read("graph_vg.pickle")

# %% Read vertices and edges
vertices = pd.read_pickle("vertices.pickle")
edges = pd.read_pickle("edges.pickle")


# %%
def extract_graph_community(graph, node_name, max_iter=5, max_nodes=20):

    if max_iter <= 0:
        return graph
    c = graph.community_multilevel()
    for sg in c.subgraphs():
        if node_name in sg.vs["name"]:
            if len(sg.vs) <= max_nodes:
                return sg
            else:
                return extract_graph_community(sg, node_name, max_iter - 1, max_nodes)


# %% Find a node
# print(
#     con.sql(
#         """
# select
# *
# from
# nodes T1
# where
# T1.title ilike '%Manic Min%'
# """
#     )
# )


# %% Iter vertices
def iter_wikipedia():
    for index, value in vertices.sort_values("page_rank", ascending=False)[
        "name"
    ].items():
        yield (value)


node_name = iterfzf(iter_wikipedia(), multi=False)

print(node_name)

# %%
sg = extract_graph_community(g, node_name, max_iter=20, max_nodes=20)

# %%
len(sg.vs)

# %%
sg.vs["label"] = sg.vs["name"]

for v in sorted(sg.vs, key=lambda x: x["page_rank"], reverse=True):
    print(v["name"], v["page_rank"])
