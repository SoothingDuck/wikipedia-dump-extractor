import glob

from . import logger
from . import session
from . import engine

from .model import ArticleCategoryLink
from .model import Category
from sqlalchemy import Index
from sqlalchemy import select

from wikipedia.dump import Dump
from wikipedia.extract import DumpFileExtractor

def load(xml_mask):
    # Alimentation des liens Article <=> Categories
    logger.info("Début : Chargement des liens Article <=> Categories")
    if session.query(ArticleCategoryLink).first() is None:
        for enwiki_articles_xml in glob.glob(xml_mask):
            logger.info("ArticleCategoryLink : Dealing with {}".format(enwiki_articles_xml))
            dump = Dump(enwiki_articles_xml)
            extractor = DumpFileExtractor(dump, dump_directory)
            i = 1
            tmp = []
            for article in extractor:
                unique_category_list = set()
                if article.redirect_title is None:
                    # Récupération des id de categories
                    for category_name in article.categories:
                        # Récupérer l'id du portail
                        result = session.execute(
                            select(Category.id).where(Category.name == category_name)
                        ).first()
                        if result is not None:
                            unique_category_list.add(result[0])

                # Ajout des portails dans la queue d'ajout 
                for category_id in unique_category_list:
                    tmp.append(
                        ArticleCategoryLink(
                            article_id = article.id,
                            category_id = category_id
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

    logger.info("Fin : Chargement des liens Article <=> Categories")

def make_index():
    # Creation d'index sur les liens de categories phase 1
    article_category_link_article_id_index = Index('article_category_link_article_id_idx', ArticleCategoryLink.article_id)
    try:
        logger.info("Début : Création index sur ArticleCategoryLink=>article_id")
        article_category_link_article_id_index.create(bind=engine)
        logger.info("Fin : Création index sur ArticleCategoryLink=>article_id")
    except:
        logger.info("Fin : Index sur ArticleCategoryLink=>article_id déjà créé")

    # Creation d'index sur les liens de portails phase 2
    article_category_link_portal_id_index = Index('article_category_link_portal_id_idx', ArticleCategoryLink.category_id)
    try:
        logger.info("Début : Création index sur ArticleCategoryLink=>category_id")
        article_category_link_portal_id_index.create(bind=engine)
        logger.info("Fin : Création index sur ArticleCategoryLink=>category_id")
    except:
        logger.info("Fin : Index sur ArticleCategoryLink=>category_id déjà créé")

if __name__ == '__main__':

    import os
    from . import dump_directory

    load(os.path.join(dump_directory, "enwiki-*-pages-articles*.bz2"))
    make_index()
