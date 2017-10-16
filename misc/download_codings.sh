#!/bin/bash
set -e

for word in `cat $1`
do
  echo "Downloading coding ${word}"
  curl -X POST -d "id=${word}" http://biobank.ctsu.ox.ac.uk/showcase/codown.cgi > coding_${word}.tsv
done

