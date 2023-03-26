import glob

from . import logger
from . import session
from . import engine

from .model import ArticlePortalLink
from .model import Portal
from sqlalchemy import Index
from sqlalchemy import select

from wikipedia.dump import Dump
from wikipedia.extract import DumpFileExtractor

def load(xml_mask):
    # Alimentation des liens Article <=> Portails
    logger.info("Début : Chargement des liens Article <=> Portails")
    if session.query(ArticlePortalLink).first() is None:
        for enwiki_articles_xml in glob.glob(xml_mask):
            logger.info("ArticlePortailLink : Dealing with {}".format(enwiki_articles_xml))
            dump = Dump(enwiki_articles_xml)
            extractor = DumpFileExtractor(dump, dump_directory)
            i = 1
            tmp = []
            for article in extractor:
                unique_portal_list = set()
                if article.redirect_title is None:
                    # Récupération des id de portail
                    for portal_name in article.portals:
                        # Récupérer l'id du portail
                        result = session.execute(
                            select(Portal.id).where(Portal.name == portal_name)
                        ).first()
                        if result is not None:
                            unique_portal_list.add(result[0])

                # Ajout des portails dans la queue d'ajout 
                for portal_id in unique_portal_list:
                    tmp.append(
                        ArticlePortalLink(
                            article_id = article.id,
                            portal_id = portal_id
                        )
                    )
                    i += 1

                # Do we commit ?
                if i > 10000:
                    # Commit when enough entries
                    session.bulk_save_objects(tmp)
                    session.commit() 
                    tmp = []
                    i = 1

            # Last commit
            session.bulk_save_objects(tmp)
            session.commit() 

    logger.info("Fin : Chargement des liens Article <=> Portails")

def make_index():
    # Creation d'index sur les liens de portails phase 1
    article_portal_link_article_id_index = Index('article_portal_link_article_id_idx', ArticlePortalLink.article_id)
    try:
        logger.info("Début : Création index sur ArticlePortalLink=>article_id")
        article_portal_link_article_id_index.create(bind=engine)
        logger.info("Fin : Création index sur ArticlePortalLink=>article_id")
    except:
        logger.info("Fin : Index sur ArticlePortalLink=>article_id déjà créé")

    # Creation d'index sur les liens de portails phase 2
    article_portal_link_portal_id_index = Index('article_portal_link_portal_id_idx', ArticlePortalLink.portal_id)
    try:
        logger.info("Début : Création index sur ArticlePortalLink=>portal_id")
        article_portal_link_portal_id_index.create(bind=engine)
        logger.info("Fin : Création index sur ArticlePortalLink=>portal_id")
    except:
        logger.info("Fin : Index sur ArticlePortalLink=>portal_id déjà créé")

if __name__ == '__main__':

    import os
    from . import dump_directory

    load(os.path.join(dump_directory, "enwiki-*-pages-articles*.bz2"))
    make_index()
