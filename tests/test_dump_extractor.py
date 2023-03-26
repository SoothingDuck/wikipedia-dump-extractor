
def test_article_retrieval(dump_extractor):
    import itertools
    from wikipedia.dump import Article

    first_article = next(iter(dump_extractor))

    assert isinstance(first_article, Article)
