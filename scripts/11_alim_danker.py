# %% Libs
from wikipedia import config

# Répertoire
import os
import duckdb

# %% Connection

con = duckdb.connect(config["database"]["name"])

# %% Alimentation de la table
con.sql(
    """
    CREATE TABLE IF NOT EXISTS page_rank_nodes(
        id INTEGER PRIMARY KEY,
        rank DOUBLE
    );
    """
)

# %% Effacement des ranks existants
con.sql("DELETE from page_rank_nodes")

# %% Insertion depuis le CSV
con.sql(
    f"""
        insert into page_rank_nodes
        select * FROM read_csv(
            'danker_all.output',
            delim='\t',
            header=false,
            columns={{
                'id': 'INTEGER',
                'rank': 'DOUBLE'
            }},
            quote='|',
            escape=''
        )
"""
)

# %% Test
con.sql(
    """
select
*
from
page_rank_nodes
order by rank desc
limit 50
"""
)
