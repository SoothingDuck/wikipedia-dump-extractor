from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
import pandas as pd

from . import engine

# declarative base class
Base = declarative_base()

# Wikipedia portal
class Portal(Base):
    __tablename__ = 'portal_list'

    id = Column(Integer, primary_key=True)
    name = Column(String)

    def edges_to_csv(self, csv_filename):

        # back and forth
        sql_query = """
        with article_portal as (
            select
            T3.id as id,
            T3.title as title,
            T3.namespace as namespace
            from
            portal_list T1 inner join
            article_portal_link T2 on (T2.portal_id = T1.id) inner join
            article T3 on (T2.article_id = T3.id)
            where
            T1.name = '{}'
            group by 1,2
        ), inbound_edges as (
            select
            T3.id as source_id,
            T3.title as source_title,
            T1.id as destination_id,
            T1.title as destination_title
            from
            article_portal T1 inner join
            article_link T2 on (T1.id = T2.link_id) inner join
            article T3 on (T2.article_id = T3.id)
            where
            T1.namespace = 0
            and
            T3.namespace = 0
            group by 1,2,3,4
        ),
        outbound_edges as (
            select
            T1.id as source_id,
            T1.title as source_title,
            T3.id as destination_id,
            T3.title as destination_title
            from
            article_portal T1 inner join
            article_link T2 on (T1.id = T2.article_id) inner join
            article T3 on (T2.link_id = T3.id)
            where
            T1.namespace = 0
            and
            T3.namespace = 0
            group by 1,2,3,4
        )
        select
        *
        from
        outbound_edges
        union all
        select
        *
        from
        inbound_edges
        """.format(self.name)

        con = engine.connect()

        df = pd.read_sql(sql_query, con)
        df.to_csv(csv_filename)


# Wikipedia category
class Category(Base):
    __tablename__ = 'category_list'

    id = Column(Integer, primary_key=True)
    name = Column(String)

# Wikipedia ArticleCategoryLink
class ArticleCategoryLink(Base):
    __tablename__ = 'article_category_link'

    article_id = Column(Integer, ForeignKey("article.id"), primary_key=True)
    category_id = Column(Integer, ForeignKey(Category.id), primary_key=True)


# Wikipedia ArticlePortalLink
class ArticlePortalLink(Base):
    __tablename__ = 'article_portal_link'

    article_id = Column(Integer, ForeignKey("article.id"), primary_key=True)
    portal_id = Column(Integer, ForeignKey(Portal.id), primary_key=True)


# Wikipedia ArticleLink
class ArticleLink(Base):
    __tablename__ = 'article_link'

    article_id = Column(Integer, ForeignKey("article.id"), primary_key=True)
    link_id = Column(Integer, ForeignKey("article.id"), primary_key=True)

# Wikipedia Article
class Article(Base):
    __tablename__ = "article"

    id = Column(Integer, primary_key=True)
    title = Column(String)
    namespace = Column(Integer)

    parents = relationship(
        "Article",
        secondary="article_link",
        primaryjoin=ArticleLink.link_id==id,
        secondaryjoin=ArticleLink.article_id==id,
        backref="children"
    )

    categories = relationship(
        "Category",
        secondary="article_category_link",
        backref="articles"
    )

    portals = relationship(
        "Portal",
        secondary="article_portal_link",
        backref="articles"
    )

# Wikipedia Redirection
class Redirection(Base):
    __tablename__ = "redirection"

    id = Column(Integer, primary_key=True)
    title = Column(String)
    redirect_title = Column(String)
    namespace = Column(Integer)


if __name__ == '__main__':

    from . import logger
    from . import engine

    logger.info("Début : Création du modèle")
    Base.metadata.create_all(engine)
    logger.info("Fin : Création du modèle")
