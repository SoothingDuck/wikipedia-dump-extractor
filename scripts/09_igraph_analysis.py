# %% Connection
import pandas as pd
import duckdb
import igraph as ig

con = duckdb.connect("../wiki.db")

# nodes
# category_type
# category_node_assoc
# portal_type
# portal_node_assoc
# redirections
# links_nodes

# %% Print portal_type
con.sql(
    """
select
*
from
portal_type
limit 5
"""
)

# %%
print(
    con.sql(
        """
select
*
from
portal_node_assoc
limit 5
"""
    )
)

print(
    con.sql(
        """
select
*
from
nodes
limit 5
"""
    )
)

# nodes request
vertices = con.sql(
    """
select
T3.title as name
from
portal_type T1 inner join
portal_node_assoc T2 on (T1.id = T2.portal_id) inner join
nodes T3 on (T2.node_id = T3.id)
where
T1.name ilike '%video game%'
and
T3.namespace = 0
group by 1
"""
).to_df()

print(vertices)

# edge request
edges = con.sql(
    """
with node_selection as (
    select
    T3.id,
    T3.title
    from
    portal_type T1 inner join
    portal_node_assoc T2 on (T1.id = T2.portal_id) inner join
    nodes T3 on (T2.node_id = T3.id)
    where
    T1.name ilike '%video game%'
    and
    T3.namespace = 0
    group by 1,2
),
edges as (
    select
    T2.title as source_name,
    T3.title as destination_name
    from
    links_nodes T1 inner join
    node_selection T2 on (T1.source_node_id = T2.id) inner join
    node_selection T3 on (T1.destination_node_id = T3.id)
    group by 1,2
)
select
source_name,
destination_name
from
edges
"""
).to_df()

print(edges)

# Graph
g = ig.Graph.DataFrame(edges=edges, directed=True, vertices=vertices, use_vids=False)
