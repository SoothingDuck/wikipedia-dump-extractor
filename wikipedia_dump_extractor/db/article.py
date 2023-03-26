import glob

from . import logger
from . import session
from . import engine

from .model import Article
from sqlalchemy import Index

from wikipedia.dump import Dump
from wikipedia.extract import DumpFileExtractor

def load(xml_mask):
    # Chargement Article si vide
    logger.info("Début : Chargement des articles")
    if session.query(Article).first() is None:
        for enwiki_articles_xml in glob.glob(xml_mask):
            logger.info("Article : Dealing with {}".format(enwiki_articles_xml))
            dump = Dump(enwiki_articles_xml)
            extractor = DumpFileExtractor(dump, dump_directory)
            i = 1
            tmp = []
            for article in extractor:
                # Only article which are not redirections
                if article.redirect_title is None:
                    tmp.append(Article(
                        id=article.id,
                        title=article.title,
                        namespace=article.ns))
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
    logger.info("Fin : Chargement des articles")

def make_index():
    # Creation d'index sur le nom de l'article
    article_title_index = Index('article_title_idx', Article.title)
    try:
        logger.info("Début : Création index sur Article=>Title")
        article_title_index.create(bind=engine)
        logger.info("Fin : Création index sur Article=>Title")
    except:
        logger.info("Fin : Index sur Article=>Title déjà créé")

if __name__ == '__main__':

    import os
    from . import dump_directory

    load(os.path.join(dump_directory, "enwiki-*-pages-articles*.bz2"))
    make_index()
