
def test_article_retrieval(dump_extractor):

    from wikipedia.etl.dump import Article

    first_article = next(iter(dump_extractor))

    assert isinstance(first_article, Article)
