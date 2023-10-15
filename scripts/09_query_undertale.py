#%% Connection
import duckdb

con = duckdb.connect('wiki.db')

#%%
con.sql("""
        select
        T3.title
        from
        nodes T1 inner join 
        links_nodes T2 on (T1.id = T2.destination_node_id) inner join
        nodes T3 on (T3.id = T2.source_node_id)
        where
        T1.title = 'Undertale'
        """)
