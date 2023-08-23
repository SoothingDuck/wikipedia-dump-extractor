import glob

from . import logger
from . import session
from . import engine

from .model import ArticleLink
from .model import Article
from .model import Redirection
from sqlalchemy import Index
from sqlalchemy import select

from wikipedia.dump import Dump
from wikipedia.etl.extract import DumpFileExtractor


def load(xml_mask):
    # Alimentation des liens Article <=> Article
    logger.info("Début : Chargement des liens Article <=> Article")
    if session.query(ArticleLink).first() is None:
        for enwiki_articles_xml in glob.glob(xml_mask):
            logger.info("En Cours : Traitement de {}".format(enwiki_articles_xml))
            dump = Dump(enwiki_articles_xml)
            extractor = DumpFileExtractor(dump, dump_directory)
            i = 1
            tmp = []
            for article in extractor:
                unique_article_list = set()
                if article.redirect_title is None:
                    # Récupération des id des liens
                    for link_name in article.links:
                        # Récupérer l'id du lien
                        result = session.execute(
                            select(Article.id).where(Article.title == link_name)
                        ).first()
                        if result is not None:
                            unique_article_list.add(result[0])
                        else:
                            # Test si redirection
                            result_redirection = session.execute(
                                select(Redirection.redirect_title).where(
                                    Redirection.title == link_name
                                )
                            ).first()
                            if result_redirection is not None:
                                # Trouver l'article
                                result_redirection_article = session.execute(
                                    select(Article.id).where(
                                        Article.title == result_redirection[0]
                                    )
                                ).first()
                                if result_redirection_article is not None:
                                    unique_article_list.add(
                                        result_redirection_article[0]
                                    )

                # Ajout des liens dans la queue d'ajout
                for link_id in unique_article_list:
                    tmp.append(ArticleLink(article_id=article.id, link_id=link_id))
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

    logger.info("Fin : Chargement des liens Article <=> Article")


def make_index():
    # Creation d'index sur les liens phase 1
    article_link_article_id_index = Index(
        "article_link_article_id_idx", ArticleLink.article_id
    )
    try:
        logger.info("Début : Création index sur ArticleLink=>article_id")
        article_link_article_id_index.create(bind=engine)
        logger.info("Fin : Création index sur ArticleLink=>article_id")
    except:
        logger.info("Fin : Index sur ArticleLink=>article_id déjà créé")

    # Creation d'index sur les liens phase 2
    article_link_link_id_index = Index("article_link_link_id_idx", ArticleLink.link_id)
    try:
        logger.info("Début : Création index sur ArticleLink=>link_id")
        article_link_link_id_index.create(bind=engine)
        logger.info("Fin : Création index sur ArticleLink=>link_id")
    except:
        logger.info("Fin : Index sur ArticleLink=>link_id déjà créé")


if __name__ == "__main__":
    import os
    from . import dump_directory

    load(os.path.join(dump_directory, "enwiki-*-pages-articles*.bz2"))
    make_index()
