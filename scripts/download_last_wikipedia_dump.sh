#!/usr/bin/bash

LANG=$1

if [ "$1" == "" ]
then
  echo "Usage : $0 LANG"
  exit 0
fi

# Identify dump files
DUMPURL_BASE="https://dumps.wikimedia.org/${LANG}wiki/latest/"
DUMPFILE_LIST=$(curl -q ${DUMPURL_BASE} 2>&1 | grep "href=\"${LANG}" | grep "latest-pages-articles[0-9]" | awk -F'"' '{ print $2 }' | grep -v "\-rss" | sort)

# Setting destination directory
DESTINATION_DIR="DATA/dump/${LANG}"
mkdir -p ${DESTINATION_DIR}

# Download dump files if not already done
# Don't bother doing it in parallel as the site allows only two simultaneous sessions (and we want to be nice :-))
for DUMPFILE in ${DUMPFILE_LIST}
do
  DUMPURL="${DUMPURL_BASE}${DUMPFILE}"

  echo "Downloading ${DUMPFILE}"
  curl -o ${DESTINATION_DIR}/${DUMPFILE} -L -O -C - ${DUMPURL}

done

