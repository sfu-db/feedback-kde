import argparse
import psycopg2
import sys
import time
import datasets
import pandas as pd

def error_metric(est_card, card):
    # both + 1 in case est_card or card being 0
    if est_card > card:
        return (est_card + 1) / (card + 1)
    else:
        return (card + 1) / (est_card + 1)

def load_test_gt(gt_file):
    df = pd.read_csv(gt_file)
    print('load test ground truth from {}'.format(gt_file))
    return df.values.reshape(-1)

# Helper function to load the query workloads.
def prepareWorkload(args):
    # Filenames used to load / record the generated workloads.
    # Lists for the workloads.
    testworkload = []
    trainworkload = []
    f = open(args.trainfile)
    for query in f:
        trainworkload.append(query)
    f.close()
    f = open(args.testfile)
    for query in f:
        testworkload.append(query)
    f.close()
    print ("done!")
    return (trainworkload, testworkload)

# Helper function to build the estimator model on the given table.
def analyzeTable(args, table):
  sys.stdout.write("\tBuilding estimator ... ")
  sys.stdout.flush()
  ts = time.time()
  # Set the statistics target for all columns
  stat_cnt = 100
  if args.model == "none":
    stat_cnt = 1
  for i in range(len(table.columns)):
    query = "ALTER TABLE %s ALTER COLUMN %s SET STATISTICS %i;" % (table.pg_name, table.columns[i].pg_name, stat_cnt)
    cur.execute(query)
  # Trigger reanalyzing the table.
  query = "ANALYZE %s(" % table.pg_name
  for i in range(len(table.columns)):
    if (i>0):
      query += ", %s" % table.columns[i].pg_name
    else:
      query += "%s" % table.columns[i].pg_name
  query += ");"
  cur.execute(query)
  # For KDE, we want to restore the sample to avoid
  if "kde" in args.model:
    sample_file = "/tmp/sample_%s.csv" % args.dbname
    if args.replay_experiment:
      # Import the sample and make sure the bandwidth is re-optimized.
      cur.execute("SELECT kde_import_sample('%s', '%s');" % (table, sample_file))
      cur.execute("SELECT kde_reset_bandwidth('%s');" % table)
    else:
      # Export the sample.
      cur.execute("SELECT kde_dump_sample('%s', '%s');" % (table, sample_file))
  print("done (took %.2f ms)!" % (1000*(time.time() - ts)))

# Define and parse the command line arguments:
parser = argparse.ArgumentParser()
    # Database connectivity arguments.
parser.add_argument("--dbname", action="store", required=True, help="Database to which the script will connect.")
parser.add_argument("--port", action="store", type=int, default=6667, help="Port of the postmaster.")
    # Arguments to specify the experiment.
parser.add_argument("--replay_experiment", action="store_true", help="Should the script [replay] the last recorded experiment?")
parser.add_argument("--trainfile", action="store", help="From which file should we take the training queries?.")
parser.add_argument("--testfile", action="store", help="From which file should we take the test queries?.")
parser.add_argument("--gtfile", action="store", help="From which file should we take the test ground truth?.")
    # Arguments to specify the model.
parser.add_argument("--model", action="store", choices=["none", "postgres", "stholes", "kde_heuristic", "kde_adaptive", "kde_batch"], default="none", help="Which model should be tested?")
parser.add_argument("--modelsize", action="store", required=True, type=int, help="For KDE: How many rows are used in the underlying sample. For STHoles: How many points are used to build the model?")
parser.add_argument("--logbw", action="store_true", help="Use a logarithmic bandwidth representation.")
    # General arguments
parser.add_argument("--log", action="store", required=True, help="Where to append the experimental results?")

args = parser.parse_args()

# First, open the connection to Postgres.
conn = psycopg2.connect("dbname=%s host=localhost port=%i user=card" % (args.dbname, args.port))
conn.set_session('read uncommitted', autocommit=True)
cur = conn.cursor()

# Make sure that debug mode is deactivated and that all model traces are removed (unless we want to reuse the model):
cur.execute("SELECT setseed(0.1234);")
cur.execute("SET kde_debug TO false;")
cur.execute("SET ocl_use_gpu TO true;")
cur.execute("SET kde_error_metric TO Quadratic;")
cur.execute("DELETE FROM pg_kdefeedback;")

#Uncomment to attach gdb
#cur.execute("select pg_backend_pid();")
#print cur.fetchone()
#time.sleep(20)

# Now prepare the workloads:
(training_workload, testing_workload) = prepareWorkload(args)
print('load train from {}({}), load test from {}({})'.format(args.trainfile, len(training_workload), args.testfile, len(testing_workload)))

# load ground truth for test
labels = load_test_gt(args.gtfile)

# load table
table = datasets.LoadForest()

# Remove all existing model traces if we don't reuse the model.
cur.execute("DELETE FROM pg_kdemodels;")
cur.execute("DELETE FROM pg_kdefeedback;")
cur.execute("SELECT pg_stat_reset();")

# Make sure we only run training queries if we have an adaptive model:
if (args.model in ("kde_heuristic", "postgres", "none")):
    training_workload = []
# Set required KDE model parameters.
if "kde" in args.model:
    # KDE-specific parameters.
    cur.execute("SET kde_samplesize TO %i;" % args.modelsize)
    cur.execute("SET kde_enable TO true;")
    if args.logbw:
        cur.execute("SET kde_bandwidth_representation TO Log;")

# Initialize the training phase for batch, adaptive and stholes:
if (args.model == "kde_batch" ):
    cur.execute("SET kde_collect_feedback TO true;")
elif (args.model == "kde_adaptive"):
    # Build an initial model that is being trained.
    cur.execute("SET kde_enable_adaptive_bandwidth TO true;")
    cur.execute("SET kde_minibatch_size TO 10;")
    cur.execute("SET kde_online_optimization_algorithm TO rmsprop;")
    analyzeTable(args, table)
elif (args.model == "stholes"):
    # STHoles-specific parameters.
    cur.execute("SET stholes_hole_limit TO %i;" % (args.modelsize / 2))
    cur.execute("SET stholes_enable TO true;")
    analyzeTable(args, table)

# Run the training workload.
if training_workload:
    sys.stdout.write("\tRunning training queries ... ")
    sys.stdout.flush()
    ts = time.time()
    for query in training_workload:
       cur.execute(query)
    print("done (took %.2f ms)!" % (1000*(time.time() - ts)))

# Build batch models based on the collected feedback.
if (args.model == "kde_batch"):
    cur.execute("SET kde_collect_feedback TO false;") # We don't need further feedback collection.
    cur.execute("SET kde_enable_bandwidth_optimization TO true;")
    cur.execute("SET kde_optimization_feedback_window TO %i;" % len(training_workload))
# KDE Adaptive and STHOles have already built their model, now we trigger building for all other models.
if (args.model != "stholes" and args.model != "kde_adaptive"):
    analyzeTable(args, table)

# Ok, we are all set. Run the experiments!
f = open(args.log, "w")
sys.stdout.write("\tRunning test queries ... ")
sys.stdout.flush()
ts = time.time()
for query, actual in zip(testing_workload, labels):
    cur.execute("EXPLAIN(format json) %s" % query)
    res = cur.fetchall()
    predicted = res[0][0][0]['Plan']['Plan Rows']
    f.write("{}-{},{:.2f},{},{}\n".format(args.model, args.modelsize, error_metric(predicted, actual), predicted, actual))
print("done (took %.2f ms)!" % (1000*(time.time() - ts)))

# Clean up
f.close()
conn.close()
