import pytest

@pytest.fixture(scope="module")
def wikipedia_dump_site():
    import os
    from wikipedia.dump import Site
    site = Site("en", "20201101")
    return site

@pytest.fixture(scope="module")
def dump_wikipedia_extract(wikipedia_dump_site):
    import os
    from wikipedia.dump import Dump
    dump = Dump(os.path.join("tests", "DATA", "enwiki-test-extract.xml.bz2"))
    return dump

@pytest.fixture(scope="module")
def dump_wikipedia(wikipedia_dump_site):
    import os
    from wikipedia.dump import Dump
    dump = Dump(os.path.join("tests", "DATA", "enwiki-test.xml.bz2"))
    return dump

@pytest.fixture(scope="module")
def dump_extractor_extract(dump_wikipedia_extract):
    import os
    from wikipedia.extract import DumpFileExtractor
    return DumpFileExtractor(dump_wikipedia_extract, os.path.join("tests", "DATA", "extract"))

@pytest.fixture(scope="module")
def dump_extractor(dump_wikipedia):
    import os
    from wikipedia.extract import DumpFileExtractor
    return DumpFileExtractor(dump_wikipedia, os.path.join("tests", "DATA", "extract"))
