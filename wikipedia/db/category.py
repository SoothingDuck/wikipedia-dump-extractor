
from . import logger
from . import session
from . import engine

from .model import Category
from sqlalchemy import Index

def load(csv_filename):
    # Chargement Categories si vide
    logger.info("Début : Chargement des catégories")
    if session.query(Category).first() is None:
        i = 1
        data_list = []
        with open(csv_filename, "r") as f:
            j = 0
            for line in f:
                category_name = line.strip().split(";")[0]
                data_list.append(Category(name=category_name))
                i += 1
                
                # Commit régulier
                if i > 10000:
                    session.bulk_save_objects(data_list)
                    session.commit()
                    data_list = []
                    i = 1
                    j += 1
                    logger.info("Category : log du batch {} de 10000 lignes".format(j))

        # Last commit
        session.bulk_save_objects(data_list)
        session.commit()
    logger.info("Fin : Chargement des catégories")

def make_index():
    # Creation d'index sur les nom de categories
    category_name_index = Index('category_name_idx', Category.name)
    try:
        logger.info("Début : Création index sur Category=>Name")
        category_name_index.create(bind=engine)
        logger.info("Fin : Création index sur Category=>Name")
    except:
        logger.info("Fin : Index sur Category=>Name déjà créé")

if __name__ == '__main__':

    import os
    from . import dump_directory

    load(os.path.join(dump_directory, "categories_list_count.csv")) 
    make_index()
