#%% Connection
import duckdb

con = duckdb.connect('wiki.db')

######### NODES ########################
#%% Création de la table des nodes
con.sql("""
CREATE TABLE IF NOT EXISTS nodes(
    id INTEGER PRIMARY KEY,
    title VARCHAR,
    namespace INTEGER
);
""")

#%% Effacement des nodes
con.sql("DELETE from nodes")

#%% Insertion depuis le CSV
con.sql("""
        insert into nodes
        select * FROM read_csv(
            'DATA/dump/fr/nodes/*.csv',
            delim=',',
            header=true,
            columns={
                'id': 'INTEGER',
                'title': 'VARCHAR',
                'namespace': 'INTEGER'
            },
            quote='|'
        )
""")

######### INFOBOXES ########################
#%% Création de la table des types d'infoboxes
con.sql("""
CREATE TABLE IF NOT EXISTS infobox_type(
    id INTEGER PRIMARY KEY,
    name VARCHAR
);
""")

#%% Effacement des nodes
con.sql("DELETE from infobox_type")

#%% Insertion depuis le CSV
con.sql("""
        insert into infobox_type
        select
        row_number() over () as id,
        T.infobox as name
        from
        (
            select 
            infobox,
            count(*)
            FROM read_csv(
                'DATA/dump/fr/infoboxes/*.csv',
                delim=',',
                header=true,
                columns={
                    'id': 'INTEGER',
                    'title': 'VARCHAR',
                    'infobox': 'VARCHAR'
                },
                quote='|'
            )
            group by 1
        ) T
""")
