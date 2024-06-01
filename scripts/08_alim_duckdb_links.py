# Libs
from wikipedia import config

# Répertoire
import os
import glob
import duckdb

program_directory = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..")
os.chdir(program_directory)

lang = config["default"]["lang"]

# %% Connection

con = duckdb.connect(config["database"]["name"])

######### Redirections ########################
# %% Création de la table des nodes
con.sql(
    """
CREATE TABLE IF NOT EXISTS redirections(
    article_title VARCHAR,
    redirection_title VARCHAR,
    PRIMARY KEY (article_title, redirection_title)
);
"""
)

# %% Effacement des nodes
con.sql("DELETE from redirections")

# %% Insertion depuis le CSV
con.sql(
    f"""
        insert into redirections
        select article_title, redirection_title FROM read_csv(
            '{config["default"]["data_directory"]}/dump/{lang}/redirections/*.csv',
            delim=',',
            header=true,
            columns={{
                'article_id': 'INTEGER',
                'article_title': 'VARCHAR',
                'redirection_title': 'VARCHAR'
            }},
            quote='|',
            ignore_errors=true
        )
        where
        article_title is not null
        and
        redirection_title is not null
        and
        redirection_title not like '%Greek diacritics%'
        group by 1,2
"""
)

######### Links ########################
# %% Création de la table des liens
con.sql(
    """
CREATE TABLE IF NOT EXISTS links_nodes(
    source_node_id INTEGER,
    destination_node_id INTEGER,
    PRIMARY KEY (source_node_id, destination_node_id)
);
"""
)

# %% Effacement des nodes
con.sql("DELETE from links_nodes")

# %% Insertion des liens avec et sans redirections

for filename in sorted(
    glob.glob(
        os.path.join(
            config["default"]["data_directory"], "dump", lang, "links", "*.csv"
        )
    )
):
    print("Traitement de {}".format(filename))
    con.sql(
        """
        insert into links_nodes
        select
        source_node_id,
        destination_node_id
        from
        (
            select 
            T1.article_id as source_node_id,
            T2.id as destination_node_id
            FROM read_csv(
                '"""
        + filename
        + """',
                delim=',',
                header=true,
                columns={
                    'article_id': 'INTEGER',
                    'link_title': 'VARCHAR'
                },
                quote='|',
                escape=''
            ) T1 inner join nodes T2 on (T1.link_title = T2.title)
            union all
            select 
            T1.article_id as source_node_id,
            T3.id as destination_id
            FROM read_csv(
                '"""
        + filename
        + """',
                delim=',',
                header=true,
                columns={
                    'article_id': 'INTEGER',
                    'link_title': 'VARCHAR'
                },
                quote='|',
                escape=''
            ) T1 inner join
            redirections T2 on (T1.link_title = T2.article_title) inner join
            nodes T3 on (T2.redirection_title = T3.title)
        ) T
        group by 1,2
    """
    )


# %%
