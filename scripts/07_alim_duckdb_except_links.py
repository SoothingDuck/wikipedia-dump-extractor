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
            'DATA/dump/en/nodes/*.csv',
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
                'DATA/dump/en/infoboxes/*.csv',
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

#%% Creation infobox
con.sql("""
CREATE TABLE IF NOT EXISTS infobox_node_assoc(
    node_id INTEGER,
    infobox_id INTEGER,
    PRIMARY KEY(node_id, infobox_id)
);
""")

#%% Suppression des liens infobox
con.sql("DELETE FROM infobox_node_assoc;")

#%% insertion des infoboxes
con.sql("""
    insert into infobox_node_assoc
    select 
    T1.id as node_id,
    T3.id as infobox_id
    FROM read_csv(
        'DATA/dump/en/infoboxes/*.csv',
        delim=',',
        header=true,
        columns={
            'id': 'INTEGER',
            'title': 'VARCHAR',
            'infobox': 'VARCHAR'
        },
        quote='|'
    ) T1 left outer join nodes T2 on (T1.title = T2.title) inner join infobox_type T3 on (T1.infobox = T3.name)
    group by 1,2
""")

#%% Requête jeux vidéos
con.sql("""
        select
        T1.name,
        count(*)
        from
        infobox_type T1 inner join
        infobox_node_assoc T2 on (T1.id = T2.infobox_id)
        where
        name like '%jeu%'
        and
        name like '%vidéo%'
        group by 1
        having count(*) > 5
        order by 2 desc
""")

#%% Série de jeux vidéos
con.sql("""
        select
        T3.title
        from
        infobox_type T1 inner join
        infobox_node_assoc T2 on (T1.id = T2.infobox_id) inner join
        nodes T3 on (T2.node_id = T3.id)
        where
        T1.name = 'jeu vidéo'
""")

######### CATEGORIES ########################
#%% Création de la table des types de categories
con.sql("""
CREATE TABLE IF NOT EXISTS category_type(
    id INTEGER PRIMARY KEY,
    name VARCHAR
);
""")

#%% Effacement des nodes
con.sql("DELETE from category_type")

#%% Insertion depuis le CSV
con.sql("""
        insert into category_type
        select
        row_number() over () as id,
        T.category as name
        from
        (
            select 
            category,
            count(*)
            FROM read_csv(
                'DATA/dump/en/categories/*.csv',
                delim=',',
                header=true,
                columns={
                    'id': 'INTEGER',
                    'title': 'VARCHAR',
                    'category': 'VARCHAR'
                },
                quote='|'
            )
            group by 1
        ) T
""")

#%% Creation category assoc
con.sql("""
CREATE TABLE IF NOT EXISTS category_node_assoc(
    node_id INTEGER,
    category_id INTEGER,
    PRIMARY KEY(node_id, category_id)
);
""")

#%% Suppression des liens category
con.sql("DELETE FROM category_node_assoc;")

#%% insertion des categories
con.sql("""
    insert into category_node_assoc
    select 
    T1.id as node_id,
    T3.id as category_id
    FROM read_csv(
        'DATA/dump/en/categories/*.csv',
        delim=',',
        header=true,
        columns={
            'id': 'INTEGER',
            'title': 'VARCHAR',
            'category': 'VARCHAR'
        },
        quote='|'
    ) T1 left outer join nodes T2 on (T1.title = T2.title) inner join category_type T3 on (T1.category = T3.name)
    group by 1,2
""")


######### PORTALS ########################
#%% Création de la table des types de portails
con.sql("""
CREATE TABLE IF NOT EXISTS portal_type(
    id INTEGER PRIMARY KEY,
    name VARCHAR
);
""")

#%% Effacement des nodes
con.sql("DELETE from portal_type")

#%% Insertion depuis le CSV
con.sql("""
        insert into portal_type
        select
        row_number() over () as id,
        T.portal as name
        from
        (
            select 
            portal,
            count(*)
            FROM read_csv(
                'DATA/dump/en/portals/*.csv',
                delim=',',
                header=true,
                columns={
                    'id': 'INTEGER',
                    'title': 'VARCHAR',
                    'portal': 'VARCHAR'
                },
                quote='|'
            )
            group by 1
        ) T
""")

#%% Creation portal assoc
con.sql("""
CREATE TABLE IF NOT EXISTS portal_node_assoc(
    node_id INTEGER,
    portal_id INTEGER,
    PRIMARY KEY(node_id, portal_id)
);
""")

#%% Suppression des liens portal
con.sql("DELETE FROM portal_node_assoc;")

#%% insertion des portails
con.sql("""
    insert into portal_node_assoc
    select 
    T1.id as node_id,
    T3.id as portal_id
    FROM read_csv(
        'DATA/dump/en/portals/*.csv',
        delim=',',
        header=true,
        columns={
            'id': 'INTEGER',
            'title': 'VARCHAR',
            'portal': 'VARCHAR'
        },
        quote='|'
    ) T1 left outer join nodes T2 on (T1.title = T2.title) inner join portal_type T3 on (T1.portal = T3.name)
    group by 1,2
""")

#%% Undertale
con.sql("""
        select
        *
        from
        nodes T1 inner join
        portal_node_assoc T2 on (T1.id = T2.node_id) inner join
        portal_type T3 on (T2.portal_id = T3.id)
        where
        T1.title = 'Undertale'
        """)

#%% Catégorie Jeu vidéo de rôle
con.sql("""
        select
        *
        from
        portal_type T3 inner join
        portal_node_assoc T2 on (T2.portal_id = T3.id) inner join
        nodes T1 on (T2.node_id = T1.id)
        where
        T3.name = 'Portail:jeu vidéo'
        """)

