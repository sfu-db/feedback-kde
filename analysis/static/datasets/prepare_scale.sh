#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $DIR/../../conf.sh

DATASETS=(bike forest set1 genhist_set2 power protein)
QUERIES=2500

# Create the scaled datasets and query workloads.
$PYTHON $DIR/scaleDatasets.py --dbname=$PGDATABASE --port=$PGPORT
for dataset in "${DATASETS[@]}" ; do
  source $dataset/tables.sh
  for table in "${TABLES[@]}"; do
    cd $dataset/queries
    $PYTHON $DIR/scaleExperiments.py                 \
      --dbname=$PGDATABASE --port=$PGPORT           \
      --queryfile=${table}_ut_0.01.sql
    $PYTHON $DIR/scaleExperiments.py                 \
      --dbname=$PGDATABASE --port=$PGPORT           \
      --queryfile=${table}_uv_0.01.sql
    $PYTHON $DIR/scaleExperiments.py                 \
      --dbname=$PGDATABASE --port=$PGPORT           \
      --queryfile=${table}_dt_0.01.sql
    $PYTHON $DIR/scaleExperiments.py                 \
      --dbname=$PGDATABASE --port=$PGPORT           \
      --queryfile=${table}_dv_0.01.sql
    cd - 2&>/dev/null
  done
done
