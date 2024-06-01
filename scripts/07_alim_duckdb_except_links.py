# %% Libs
from wikipedia import config

# Répertoire
import os
import duckdb

program_directory = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..")
os.chdir(program_directory)

# %% Get config
sync_list = [x.strip() for x in config["database"]["sync_list"].split(",")]
lang = config["default"]["lang"]

# %% Connection

con = duckdb.connect(config["database"]["name"])

# %% Nodes
if "node" in sync_list:
    # %% Création de la table des nodes
    ######### NODES ########################
    con.sql(
        """
    CREATE TABLE IF NOT EXISTS nodes(
        id INTEGER PRIMARY KEY,
        title VARCHAR,
        namespace INTEGER
    );
    """
    )

    # %% Effacement des nodes
    con.sql("DELETE from nodes")

    # %% Insertion depuis le CSV
    con.sql(
        f"""
            insert into nodes
            select * FROM read_csv(
                '{config["default"]["data_directory"]}/dump/{lang}/nodes/*.csv',
                delim=',',
                header=true,
                columns={{
                    'id': 'INTEGER',
                    'title': 'VARCHAR',
                    'namespace': 'INTEGER'
                }},
                quote='|',
                escape=''
            )
    """
    )

# %% Infoboxes
if "infobox" in sync_list:
    ######### INFOBOXES ########################
    # %% Création de la table des types d'infoboxes
    con.sql(
        """
    CREATE TABLE IF NOT EXISTS infobox_type(
        id INTEGER PRIMARY KEY,
        name VARCHAR
    );
    """
    )

    # %% Effacement des nodes
    con.sql("DELETE from infobox_type")

    # %% Insertion depuis le CSV
    con.sql(
        f"""
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
                    '{config["default"]["data_directory"]}/dump/{lang}/infoboxes/*.csv',
                    delim=',',
                    header=true,
                    columns={{
                        'id': 'INTEGER',
                        'title': 'VARCHAR',
                        'infobox': 'VARCHAR'
                    }},
                    quote='|',
                    escape='',
                    ignore_errors=true
                )
                group by 1
            ) T
    """
    )

    # %% Creation infobox
    con.sql(
        """
    CREATE TABLE IF NOT EXISTS infobox_node_assoc(
        node_id INTEGER,
        infobox_id INTEGER,
        PRIMARY KEY(node_id, infobox_id)
    );
    """
    )

    # %% Suppression des liens infobox
    con.sql("DELETE FROM infobox_node_assoc;")

    # %% insertion des infoboxes
    con.sql(
        f"""
        insert into infobox_node_assoc
        select
        T1.id as node_id,
        T3.id as infobox_id
        FROM read_csv(
            '{config["default"]["data_directory"]}/dump/{lang}/infoboxes/*.csv',
            delim=',',
            header=true,
            columns={{
                'id': 'INTEGER',
                'title': 'VARCHAR',
                'infobox': 'VARCHAR'
            }},
            quote='|',
            escape='',
            ignore_errors=true
        ) T1 left outer join nodes T2 on (T1.title = T2.title) inner join infobox_type T3 on (T1.infobox = T3.name)
        group by 1,2
    """
    )

# %% Categories
if "category" in sync_list:
    # %% Création de la table des types de categories
    ######### CATEGORIES ########################
    con.sql(
        """
    CREATE TABLE IF NOT EXISTS category_type(
        id INTEGER PRIMARY KEY,
        name VARCHAR
    );
    """
    )

    # %% Effacement des nodes
    con.sql("DELETE from category_type")

    # %% Insertion depuis le CSV
    con.sql(
        f"""
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
                    '{config["default"]["data_directory"]}/dump/{lang}/categories/*.csv',
                    delim=',',
                    header=true,
                    columns={{
                        'id': 'INTEGER',
                        'title': 'VARCHAR',
                        'category': 'VARCHAR'
                    }},
                    quote='|',
                    escape=''
                )
                group by 1
            ) T
    """
    )

    # %% Creation category assoc
    con.sql(
        """
    CREATE TABLE IF NOT EXISTS category_node_assoc(
        node_id INTEGER,
        category_id INTEGER,
        PRIMARY KEY(node_id, category_id)
    );
    """
    )

    # %% Suppression des liens category
    con.sql("DELETE FROM category_node_assoc;")

    # %% insertion des categories (kernel crash)
    con.sql(
        f"""
        insert into category_node_assoc
        select
        T1.id as node_id,
        T3.id as category_id
        FROM read_csv(
            '{config["default"]["data_directory"]}/dump/{lang}/categories/*.csv',
            delim=',',
            header=true,
            columns={{
                'id': 'INTEGER',
                'title': 'VARCHAR',
                'category': 'VARCHAR'
            }},
            quote='|',
            escape=''
        ) T1 left outer join nodes T2 on (T1.title = T2.title) inner join category_type T3 on (T1.category = T3.name)
        group by 1,2
    """
    )

# %% Portals
if "portal" in sync_list:
    ######### PORTALS ########################
    # %% Création de la table des types de portails
    con.sql(
        """
    CREATE TABLE IF NOT EXISTS portal_type(
        id INTEGER PRIMARY KEY,
        name VARCHAR
    );
    """
    )

    # %% Effacement des nodes
    con.sql("DELETE from portal_type")

    # %% Insertion depuis le CSV
    con.sql(
        f"""
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
                    '{config["default"]["data_directory"]}/dump/{lang}/portals/*.csv',
                    delim=',',
                    header=true,
                    columns={{
                        'id': 'INTEGER',
                        'title': 'VARCHAR',
                        'portal': 'VARCHAR'
                    }},
                    quote='|',
                    escape=''
                )
                group by 1
            ) T
    """
    )

    # %% Creation portal assoc
    con.sql(
        """
    CREATE TABLE IF NOT EXISTS portal_node_assoc(
        node_id INTEGER,
        portal_id INTEGER,
        PRIMARY KEY(node_id, portal_id)
    );
    """
    )

    # %% Suppression des liens portal
    con.sql("DELETE FROM portal_node_assoc;")

    # %% insertion des portails
    con.sql(
        f"""
        insert into portal_node_assoc
        select
        T1.id as node_id,
        T3.id as portal_id
        FROM read_csv(
            '{config["default"]["data_directory"]}/dump/{lang}/portals/*.csv',
            delim=',',
            header=true,
            columns={{
                'id': 'INTEGER',
                'title': 'VARCHAR',
                'portal': 'VARCHAR'
            }},
            quote='|',
            escape=''
        ) T1 left outer join nodes T2 on (T1.title = T2.title) inner join portal_type T3 on (T1.portal = T3.name)
        group by 1,2
    """
    )
