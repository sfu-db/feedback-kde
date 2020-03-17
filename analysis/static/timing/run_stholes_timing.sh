#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $DIR/../../conf.sh
cd $DIR

# Some general parameters.
REPETITIONS=1
QUERIES=100

DIMENSIONS=(8)
MODELSIZES=(512 1024 2048 4096 8192 16384 32768 65536 131072 262144 524288 1048576)
LOGPREFIX=$DIR/../../evaluation/timing/

for dimension in "${DIMENSIONS[@]}"; do
    echo "Running for $dimension dimensions:"
    for modelsize in "${MODELSIZES[@]}"; do
      echo "  Running with modelsize $modelsize:"
      $POSTGRES -D $PGDATAFOLDER -p $PGPORT >> postgres.log 2>&1 &
      PGPID=$!
      sleep 2
      for i in $(seq 1 $REPETITIONS); do
         echo "    Repetition $i of $REPETITIONS:"
            
         $PYTHON $DIR/runSTHolesTimingExperiment.py                \
            --dbname=$PGDATABASE --port=$PGPORT                   \
            --dimensions=$dimension --log=$LOGPREFIX/result_stholes.csv \
            --modelsize=$modelsize --queries=$QUERIES 
         
      done
      kill -9 $PGPID
      sleep 2
    done
done
