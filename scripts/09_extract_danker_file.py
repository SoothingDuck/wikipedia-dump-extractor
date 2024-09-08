# %% Voir toutes les colonnes de pandas
import duckdb
import pandas as pd

pd.set_option("display.max_rows", 1000)
pd.set_option("display.max_columns", 1000)
pd.set_option("display.width", 1000)

# %% Connection
con = duckdb.connect("../wiki.db")

# %% nodes request
links = con.sql(
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
    source_node_id,
    destination_node_id
from
    RAW_EDGES
group by 1,2
order by 1,2
"""
).to_df()

# %% Write danker_output
links.to_csv(
    "../danker_links_video_games.tsv",
    sep="\t",
    header=False,
    index=False,
)
