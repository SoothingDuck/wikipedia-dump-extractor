# wikipedia-dump-extractor

A simple library to download, 
extract and transform the wikipedia articles
found in xml form in https://dumps.wikimedia.org/
for further analysis of the 
heritage of mankind

# Download

Right now the 
`download_last_wikipedia_dump.sh`
will do the job.

## For English dumps

```shell
./scripts/download_last_wikipedia.sh en
```

After a "little" wait bzipped dumps can be found
in `DATA/dump/en`

## For French dumps

```shell
./scripts/download_last_wikipedia.sh fr
```

This time dumps can be found
in `DATA/dump/fr`.

# Extraction

By default, relevant data are extracted to JSON.






