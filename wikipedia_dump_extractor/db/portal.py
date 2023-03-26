
from . import logger
from . import session
from . import engine

from .model import Portal
from sqlalchemy import Index

def load(csv_filename):
    # Chargement Portail si vide
    logger.info("Début : Chargement des portails")
    if session.query(Portal).first() is None:
        i = 1
        data_list = []
        with open(csv_filename, "r") as f:
            j = 0
            for line in f:
                portal_name = line.strip().split(";")[0]
                data_list.append(Portal(name=portal_name))
                i += 1
                
                # Commit régulier
                if i > 10000:
                    session.bulk_save_objects(data_list)
                    session.commit()
                    data_list = []
                    i = 1
                    j += 1
                    logger.info("Portal : log du batch {} de 10000 lignes".format(j))

        # Last commit
        session.bulk_save_objects(data_list)
        session.commit()
    logger.info("Fin : Chargement des portails")

def make_index():
    # Creation d'index sur les nom de portails
    portal_name_index = Index('portal_name_idx', Portal.name)
    try:
        logger.info("Début : Création index sur Portal=>Name")
        portal_name_index.create(bind=engine)
        logger.info("Fin : Création index sur Portal=>Name")
    except:
        logger.info("Fin : Index sur Portal=>Name déjà créé")

if __name__ == '__main__':

    import os
    from . import dump_directory

    load(os.path.join(dump_directory, "portal_list_count.csv")) 
    make_index()
