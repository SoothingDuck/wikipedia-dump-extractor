#%% Connection
import duckdb

con = duckdb.connect("wiki.db")

#%% Structure tables des nodes
print(con.sql(
    """
select
*
from
nodes limit 5
"""
))

#%% Structure de links_node
print(con.sql(
    """
select
*
from
links_nodes
limit 5
"""
))

#%% Comptage par popularité des lien entrants
print(con.sql("""
select
*
from
nodes T1
where
T1.title = 'Undertale'
"""
))

#%%

print(con.sql("""
select
T1.title,
T3.title
from
nodes T1 inner join
links_nodes T2 on (T1.id = T2.destination_node_id) inner join
nodes T3 on (T2.source_node_id = T3.id)
where
T1.title = 'Undertale'
"""
))
