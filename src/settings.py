############ DIRECTORIES ############
MONITOR_DIRECTORY = "/rates/"
RATES_DIR = "/rates/"
SCRIPT_PATH = "/now/src/run_queries.sh"
RESULTS_DIR_DOCKER = "/now/db/"
RESULTS_DIR = "/home/alex/ds2docker/runtime/db/"
PATH = PATH_TO_DB = "../db/"
PLOT_DIR = PATH_TO_DB + "plots/"
############ END_OF_DIRECTORIES ############
"""
@generate_query.py
Set the max_parallelism of the generated queries and the times that the each
input rate is going to increase. 
"""
MAX_PARALLELISM = 4
RATE_ITERATIONS = 5

VALID_QUERIES_NAMES = set({
    "Query1",
    "Query2",
    "Query3",
    "Query3Stateful",
    "Query5",
    #"Query8",
    "Query11"
})

VALID_CSV_SPECIFIERS = set([
    "equalmetrics", "metrics", "all", "", "extract", "_test", "_equalmetrics",
    "metricsv1", "equalv1"
])

QUERY_ARGS = {
    "Query1": {
        "exRate": 0.8,
        "srcRate": 40000,
        "psource": 1,
        "pmap": 1
    },
    "Query2": {
        "srcRate": 40000,
        "psource": 1,
        "pmap": 1
    },
    "Query3": {
        "auctionRate": 15000,
        "personRate": 8000,
        "pAuctionSource": 1,
        "pPersonSource": 1,
        "pjoin": 1,
    },
    "Query3Stateful": {
        "auctionRate": 15000,
        "personRate": 8000,
        "pAuctionSource": 1,
        "pPersonSource": 1,
        "pjoin": 1,
    },
    "Query5": {
        "srcRate": 40000,
        "psource": 1,
        "pmap": 1
    },
    "Query8": {
        "auctionRate": 15000,
        "personRate": 8000,
        "pAuctionSource": 1,
        "pPersonSource": 1,
        "pjoin": 1,
    },
    "Query11": {
        "srcRate": 40000,
        "psource": 1,
        "pmap": 1
    },
}

INCREMENT_METRICS = {
    "Query1": {
        "exRate": 0.8,
        "srcRate": 40000,
        "psource": 1,
        "pmap": 1
    },
    "Query2": {
        "srcRate": 40000,
        "psource": 1,
        "pmap": 1
    },
    "Query3": {
        "auctionRate": 5000,
        "personRate": 2000,
        "pAuctionSource": 1,
        "pPersonSource": 1,
        "pjoin": 1,
    },
    "Query3Stateful": {
        "auctionRate": 5000,
        "personRate": 2000,
        "pAuctionSource": 1,
        "pPersonSource": 1,
        "pjoin": 1,
    },
    "Query5": {
        "srcRate": 40000,
        "psource": 1,
        "pmap": 1
    },
    "Query8": {
        "auctionRate": 5000,
        "personRate": 2000,
        "pAuctionSource": 1,
        "pPersonSource": 1,
        "pjoin": 1,
    },
    "Query11": {
        "srcRate": 40000,
        "psource": 1,
        "pmap": 1
    },
}
NUM_ARGUMENTS_PER_QUERY = {
    "Query1": 4,  # exc-rate,source-rate,p-rate,p-map
    "Query2": 3,  # source-rate,p-rate,p-flatmap
    "Query3":
    5,  # auct-srcRate, person-sRate,p-auction-source,p-person-source,p-join
    "Query3Stateful":
    5,  # auct-srcRate, person-sRate,p-auction-source,p-person-source,p-join
    "Query5": 3,  # srcRate,p-bid-source,p-window
    "Query8":
    5,  # auct-srcRate, person-sRate,p-auction-source,p-person-source,p-window
    "Query11": 3,  # srcRate,p-bid-source,p-window
}

# Depicts how many rates each Query has.
RATES_PER_QUERY = {
    "Query1": 2,
    "Query2": 1,
    "Query3": 2,
    "Query3Stateful": 2,
    "Query5": 1,
    "Query8": 2,
    "Query11": 1,
}

# The name of each input metrics we increase per Query
RATES_NAMES_PER_QUERY = {
    "Query1": ["exRate", "srcRate"],
    "Query2": ["srcRate"],
    "Query3": ["auctionRate", "personRate"],
    "Query3Stateful": ["auctionRate", "personRate"],
    "Query5": ["srcRate"],
    "Query8": ["auctionRate", "personRate"],
    "Query11": ["srcRate"],
}

# The name of each input parallelism operator we increase per Query
OPERATORS_NAMES_PER_QUERY = {
    "Query1": ["psource", "pmap"],
    "Query2": ["psource", "pmap"],
    "Query3": ["pAuctionSource", "pPersonSource", "pjoin"],
    "Query3Stateful": ["pAuctionSource", "pPersonSource", "pjoin"],
    "Query5": ["psource", "pmap"],
    "Query8": ["pAuctionSource", "pPersonSource", "pjoin"],
    "Query11": ["psource", "pmap"],
}

########## PANDAS_DATAFRAMES @ analyzer.py ##########
MAIN_HEADER = [
    'operator',
    'id',
    "op_instance_id",
    "total_op_instances",
    "epoch_timestamp",
    "true_proc_rate",
    "true_output_rate",
    "observed_proc_rate",
    "observed_output_rate",
]

HEADER_PER_QUERY = {
    "Query1": ["exRate", "srcRate", "BidSource", "PMap/LSink"],
    "Query2": ["srcRate", "BidSource", "FMap/DummyLatencySink"],
    "Query3": [
        "auctionRate",
        "personRate",
        "PAuctionSource",
        "PPersonSource",
        "IncJoin/Sink",
    ],
    "Query3Stateful": [
        "auctionRate",
        "personRate",
        "PAuctionSource",
        "PPersonSource",
        "IncJoin/Sink",
    ],
    "Query5": ["srcRate", "BidsSource", "SlidingWindow/DummyLatencySink"],
    "Query8": [
        "auctionRate",
        "personRate",
        "PAuctionSource",
        "PPersonSource",
        "IncJoin/Sink",
    ],
    "Query11": ["srcRate", "BidsSource", "SessionWindow/DummyLatencySink"],
}
