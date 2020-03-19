#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $DIR/../conf.sh
cd $DIR

# Some general parameters.
REPETITIONS=1
# TRAINFILE="tiny_train.sql"
# TESTFILE="tiny_test.sql"
# GTFILE="tiny_gt.csv"
TRAINFILE="query_train.sql"
TESTFILE="query_test.sql"
GTFILE="test_gt.csv"
echo $TRAINQUERIES
echo $TESTQUERIES

# for forest dataset
MODELSIZE=5810
LOGFILE=$DIR/result.csv
PGDATABSE=forest_num

echo >> $LOGFILE

# echo "      None:"
# $PYTHON $DIR/new_run.py                                 \
#     --dbname=$PGDATABASE --port=$PGPORT                       \
#     --log=$DIR/default_result.csv --gtfile=$GTFILE                         \
#     --model=none --modelsize=$MODELSIZE                    \
#     --trainfile=$TRAINFILE --testfile=$TESTFILE   \
    #    --replay_experiment

$POSTGRES -D $PGDATAFOLDER -p $PGPORT >>  postgres.log 2>&1 &
PGPID=$!
sleep 2

# Run stholes:
echo "      STHoles:"
$PYTHON $DIR/new_run.py                                 \
    --dbname=$PGDATABASE --port=$PGPORT                       \
    --log=$DIR/stholes_result.csv  --gtfile=$GTFILE                        \
    --model=stholes --modelsize=$MODELSIZE                    \
    --trainfile=$TRAINFILE --testfile=$TESTFILE   \
    #    --replay_experiment

kill -9 $PGPID
sleep 2

# for dataset in "${DATASETS[@]}"; do
#     echo "Running experiments for $dataset:"
#     for query in $DIR/datasets/$dataset/queries/*.sql; do
#         [ -f "${query}" ] || continue
#         query_file=`basename $query`
#         echo "  Running for query file $query_file:"
#         # Reinitialize postgres (just to be safe)
#         $POSTGRES -D $PGDATAFOLDER -p $PGPORT >>  postgres.log 2>&1 &
#         PGPID=$!
#         sleep 2
#         for i in $(seq 1 $REPETITIONS); do
#            echo "    Repetition $i:"
#            TS=$SECONDS

#            # Pick a new experiment and run batch:
#            # echo "      KDE (batch):"
#            # $PYTHON $DIR/runExperiment.py                                 \
#            #    --dbname=$PGDATABASE --port=$PGPORT                       \
#            #    --queryfile=$query --log=$LOGFILE                         \
#            #    --model=kde_batch --modelsize=$MODELSIZE                  \
#            #    --trainqueries=$TRAINQUERIES --testqueries=$TESTQUERIES

#            # Run KDE heuristic:
#            # echo "      KDE (heuristic):"
#            # $PYTHON $DIR/runExperiment.py                                 \
#            #    --dbname=$PGDATABASE --port=$PGPORT                       \
#            #    --queryfile=$query --log=$LOGFILE                         \
#            #    --model=kde_heuristic --modelsize=$MODELSIZE              \
#            #    --trainqueries=$TRAINQUERIES --testqueries=$TESTQUERIES   \
#               # --replay_experiment

#            # Run KDE optimal:
#            # echo "      KDE (SCV):"
#            # $PYTHON $DIR/runExperiment.py                                 \
#            #    --dbname=$PGDATABASE --port=$PGPORT                       \
#            #    --queryfile=$query --log=$LOGFILE                         \
#            #    --model=kde_scv --modelsize=$MODELSIZE                    \
#            #    --trainqueries=$TRAINQUERIES --testqueries=$TESTQUERIES   \
#            #    --replay_experiment

#            # Run KDE adpative:
#            # echo "      KDE (adaptive):"
#            # $PYTHON $DIR/runExperiment.py                                 \
#            #    --dbname=$PGDATABASE --port=$PGPORT                       \
#            #    --queryfile=$query --log=$LOGFILE                         \
#            #    --model=kde_adaptive --modelsize=$MODELSIZE --logbw       \
#            #    --trainqueries=$TRAINQUERIES --testqueries=$TESTQUERIES   \
#            #    --replay_experiment

#            # Run stholes:
#            echo "      STHoles:"
#            $PYTHON $DIR/runExperiment.py                                 \
#               --dbname=$PGDATABASE --port=$PGPORT                       \
#               --queryfile=$query --log=$LOGFILE                         \
#               --model=stholes --modelsize=$MODELSIZE                    \
#               --trainqueries=$TRAINQUERIES --testqueries=$TESTQUERIES   \
#            #    --replay_experiment

#            # Run with Postgres Histograms:
#            #echo "      Postgres histograms:"
#            #$PYTHON $DIR/runExperiment.py                                 \
#            #   --dbname=$PGDATABASE --port=$PGPORT                       \
#            #   --queryfile=$query --log=$LOGFILE                         \
#            #   --model=postgres --modelsize=$MODELSIZE                   \
#            #   --trainqueries=$TRAINQUERIES --testqueries=$TESTQUERIES   \
#            #   --replay_experiment

#            # Run without statistics:
#            #echo "      No statistics:"
#            #$PYTHON $DIR/runExperiment.py                                 \
#            #   --dbname=$PGDATABASE --port=$PGPORT                       \
#            #   --queryfile=$query --log=$LOGFILE                         \
#            #   --model=none --modelsize=$MODELSIZE                       \
#            #   --trainqueries=$TRAINQUERIES --testqueries=$TESTQUERIES   \
#            #   --replay_experiment

#            ELAPSED=$(($SECONDS - $TS))
#            echo "    Repetition finished (took $ELAPSED seconds)!"

#         done
#         kill -9 $PGPID
#         sleep 2
#     done
# done
