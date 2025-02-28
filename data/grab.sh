#!/usr/bin/env bash

# <https://github.com/jainabhishek0406/JavaSpark_QueryExample>
#curl -fSL# -O https://github.com/jainabhishek0406/JavaSpark_QueryExample/raw/master/data/100%20Sales%20Records.csv
#curl -fSL# -O https://github.com/jainabhishek0406/JavaSpark_QueryExample/raw/master/data/1000%20Sales%20Records.csv

for f in *.csv; do
  if [[ -e "$f" ]]; then
    # handle the case of no *.wav files

    target="${f//%20/_}"
    if [ "${f}" = "$target" ]; then
      :
      echo "skip rename ${f}"
    else
      echo "$f"
      mv -f --verbose "${f}" "${target}"
    fi
  fi

done