
import pytest


@pytest.fixture
def a_redirection(dump_extractor):
    it = iter(dump_extractor)
    return next(it)

@pytest.fixture
def another_redirection(dump_extractor_extract):
    it = iter(dump_extractor_extract)
    return next(it)

@pytest.fixture
def an_article(dump_extractor):
    it = iter(dump_extractor)
    next(it)
    return next(it)

@pytest.fixture
def abraham_lincoln_article(dump_extractor):
    for article in dump_extractor:
        if article.title == "Abraham Lincoln":
            return article


@pytest.fixture
def actrius_article(dump_extractor):
    for article in dump_extractor:
        if article.title == "Actrius":
            return article


@pytest.fixture
def playtronic_article(dump_extractor_extract):
    for article in dump_extractor_extract:
        if article.title == "Playtronic":
            return article


def test_redirection(a_redirection):

    assert a_redirection.title == "AccessibleComputing"
    assert a_redirection.redirect_title == "Computer accessibility"


def test_article_abraham_lincoln(abraham_lincoln_article):

    assert abraham_lincoln_article.title == "Abraham Lincoln"

    # Links
    assert "Category:1865 deaths" not in abraham_lincoln_article.links

    # Categories
    assert "Category:1865 deaths" in abraham_lincoln_article.categories

    # Portals
    assert "Portal:American Civil War" in abraham_lincoln_article.portals

    # Infoboxes
    assert abraham_lincoln_article.infoboxes == ["officeholder", "u.s. cabinet"]

def test_article_extractor(playtronic_article):

    assert playtronic_article is not None

    assert playtronic_article.title == "Playtronic"
    assert playtronic_article.redirect_title is None

    # Portails
    assert "Portal:Video games" in playtronic_article.portals

def test_actrius(actrius_article):

    assert actrius_article.title == "Actrius"
    assert "film" in actrius_article.infoboxes

def test_article(an_article):

    assert an_article.title == "Anarchism"
    assert an_article.redirect_title is None
    assert an_article.id == 12
    assert an_article.ns == 0

    # Links
    assert "State (polity)" in an_article.links
    assert "William Godwin" in an_article.links
    assert "Max Stirner" in an_article.links

    # Categories
    assert "Category:Anarchism" in an_article.categories
    assert "Category:Left-wing politics" in an_article.categories

    # Portals
    assert "Portal:Libertarianism" in an_article.portals
    assert "Portal:Anarchism" in an_article.portals
