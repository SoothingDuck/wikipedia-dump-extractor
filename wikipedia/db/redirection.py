import glob

from wikipedia.db import logger
from wikipedia.db import session
from wikipedia.db import engine

from wikipedia.db.model import Redirection
from sqlalchemy import Index

from wikipedia.etl.dump import Dump
from wikipedia.etl.extract import DumpFileExtractor


def load(xml_mask):
    # Chargement Redirection si vide
    logger.info("Début : Chargement des redirections")
    if session.query(Redirection).first() is None:
        for enwiki_articles_xml in glob.glob(xml_mask):
            logger.info("Redirection : Dealing with {}".format(enwiki_articles_xml))
            dump = Dump(enwiki_articles_xml)
            extractor = DumpFileExtractor(dump, dump_directory)
            i = 1
            tmp = []
            for article in extractor:
                # Only article which are redirections
                if article.redirect_title is not None:
                    tmp.append(
                        Redirection(
                            id=article.id,
                            title=article.title,
                            redirect_title=article.redirect_title,
                            namespace=article.ns,
                        )
                    )
                    i += 1
                if i > 10000:
                    # Commit when enough entries
                    session.bulk_save_objects(tmp)
                    session.commit()
                    tmp = []
                    i = 1
            # Last commit
            session.bulk_save_objects(tmp)
            session.commit()
    logger.info("Fin : Chargement des redirections")


def make_index():
    # Creation d'index sur le nom des redirections
    redirection_title_index = Index("redirection_title_idx", Redirection.title)
    try:
        logger.info("Début : Création index sur Redirection=>Title")
        redirection_title_index.create(bind=engine)
        logger.info("Fin : Création index sur Redirection=>Title")
    except:
        logger.info("Fin : Index sur Redirection=>Title déjà créé")


if __name__ == "__main__":
    import os
    from wikipedia.db import dump_directory

    load(os.path.join(dump_directory, "enwiki-*-pages-articles*.bz2"))
    make_index()
