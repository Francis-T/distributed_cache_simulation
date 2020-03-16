import paho.mqtt.client as mqtt
import time
import json

from simulator.core import *
from simulator.util import *

from simulator.broker import BrokerSimulator
from simulator.node import AggNodeSimulator, ProcNodeSimulator
from simulator.cache import Cache, CacheIndex
from simulator.query_logger import QueryCompletionLogger


# Initialize a grid
grid_arr = GridArray(5, 5, min_tasks=4, max_tasks=4)

# Initialize the Cache Index
ci = CacheIndex()
ci.start()

# Start the Query Logger
print("Starting logger")
qc_logger = QueryCompletionLogger()
qc_logger.start()

# Initialize the Broker Simulator/s
bsim_list = []
for g in grid_arr.items():
    bsim_list.append( BrokerSimulator(g, enable_caching=True) )

# Start the Broker Simulator/s
print("Starting broker simulator")
for bsim in bsim_list:
    bsim.start()

# Wait for everything to shut down
for bsim in bsim_list:
    bsim.join()

qc_logger.join()

ci.join()

print("Press any key to shutdown...")
input()

