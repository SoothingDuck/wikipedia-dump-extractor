#%% Connection
import duckdb

con = duckdb.connect('wiki.db')

#%% Structure tables des nodes
con.sql("""
select
*
from
nodes limit 5
""")

#%% Structure de links_node
con.sql("""
select
*
from
links_nodes
limit 5
""")

#%% Comptage par popularité des lien entrants
con.sql("""
select
T4.id,
T4.title,
count(*)
from
(
        select
        T3.id,
        T3.title
        from
        nodes T1 inner join 
        links_nodes T2 on (T1.id = T2.destination_node_id) inner join
        nodes T3 on (T3.id = T2.source_node_id)
        where
        T1.title = 'Undertale'
        group by 1,2
) T4 inner join
links_nodes T5 on (T4.id = T5.destination_node_id)
where
T4.title like '%pain%'
group by 1,2
order by 3 desc
        """)
