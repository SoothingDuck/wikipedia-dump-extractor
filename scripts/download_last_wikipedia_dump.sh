#!/usr/bin/bash

LANG=$1

if [ "$1" == "" ]
then
  echo "Usage : $0 LANG"
  exit 0
fi

# dumps
DUMP_URL="https://dumps.wikimedia.org/${LANG}wiki/latest/${LANG}wiki-latest-pages-articles.xml.bz2"
DESTINATION_FILENAME="${LANG}wiki-latest-pages-articles.xml.bz2"

mkdir -p DATA/dump/${LANG}
curl -o DATA/dump/${LANG}/${DESTINATION_FILENAME} -L -O -C - ${DUMP_URL}

