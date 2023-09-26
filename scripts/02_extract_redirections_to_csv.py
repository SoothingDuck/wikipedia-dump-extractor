
from wikipedia.etl.tools import batch_extract

if __name__ == '__main__':
    batch_extract("fr", "redirections", "extract_redirections")