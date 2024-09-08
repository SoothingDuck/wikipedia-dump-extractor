# %% Connection
import duckdb
import igraph as ig

con = duckdb.connect("wiki.db")

# nodes
# category_type
# category_node_assoc
# portal_type
# portal_node_assoc
# redirections
# links_nodes

# %% Print TOP portal portal_type
print(
    con.sql(
        """
select
T2.name as portal_name,
count(*)
from
portal_node_assoc T1 inner join
portal_type T2 on (T1.portal_id = T2.id)
where
T2.name ilike '%video games%'
group by 1
order by 2 desc
limit 20
"""
    )
)

# %% Links
print(
    con.sql(
        """
        select
        *
        from
        links_nodes
        limit 5
        """
    )
)

# %% Les catégories
print(
    con.sql(
        """
        select
        *
        from
        category_type
        where
        name ilike '%video%'
        and
        name ilike '%game%'
        """
    )
)

# %% Les catégories
print(
    con.sql(
        """
        select
        T2.name as category_name,
        count(*)
        from
        category_node_assoc T1 inner join
        category_type T2 on (T2.id = T1.category_id)
        where
        T2.name ilike '%video%'
        and
        T2.name ilike '%game%'
        group by 1
        order by 2 desc
        limit 20
        """
    )
)

# %% nodes request
vertices = con.sql(
    """
with RAW_NODES as (
    select
    T3.id,
    T3.title as name
    from
    category_type T1 inner join
    category_node_assoc T2 on (T1.id = T2.category_id) inner join
    nodes T3 on (T2.node_id = T3.id)
    where
    T1.name ilike '%video%'
    and
    T1.name ilike '%game%'
    and
    T3.namespace = 0
    group by 1,2
), RAW_EDGES as (
    select
    T1.source_node_id,
    T1.destination_node_id,
    T2.name as source_name,
    T3.name as destination_name
    from
    links_nodes T1 inner join
    RAW_NODES T2 on (T1.source_node_id = T2.id) inner join
    RAW_NODES T3 on (T1.destination_node_id = T3.id)
    group by 1,2
)
select
T1.name as name,
min(T2.rank) as page_rank
from
RAW_NODES T1 inner join
page_rank_nodes T2 on (T1.id = T2.id)
group by 1
"""
).to_df()

vertices.to_pickle("vertices.pickle")

# %% edge request
edges = con.sql(
    """
with RAW_NODES as (
    select
    T3.id,
    T3.title as name
    from
    category_type T1 inner join
    category_node_assoc T2 on (T1.id = T2.category_id) inner join
    nodes T3 on (T2.node_id = T3.id)
    where
    T1.name ilike '%video%'
    and
    T1.name ilike '%game%'
    and
    T3.namespace = 0
    group by 1,2
), RAW_EDGES as (
    select
    T1.source_node_id,
    T1.destination_node_id,
    T2.name as source_name,
    T3.name as destination_name
    from
    links_nodes T1 inner join
    RAW_NODES T2 on (T1.source_node_id = T2.id) inner join
    RAW_NODES T3 on (T1.destination_node_id = T3.id)
    group by 1,2,3,4
)
select
source_name,
destination_name
from
RAW_EDGES
group by 1,2
"""
).to_df()

edges.to_pickle("edges.pickle")

# %% Graph
g = ig.Graph.DataFrame(edges=edges, directed=False, vertices=vertices, use_vids=False)

# %% Simplify
g = g.simplify()

# %% Save graph
g.write("graph_vg.pickle")
