#!/bin/bash
INPUTFILE="/Users/eunsoo/jupyter/data/interval-block-20181013.csv"
OUTPUTDIR="output"
spark-submit \
  --master local[4] \
  /Users/eunsoo/mariadb-docker/flatten-data-spark/target/scala-2.11/greenbutton-etl_2.11-0.1.jar \
  $INPUTFILE \
  $OUTPUTDIR >log.txt &
echo "Check the job progress in Web UI: http://localhost:4040"