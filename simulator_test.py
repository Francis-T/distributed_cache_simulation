import paho.mqtt.client as mqtt
import time
import json

from simulator.core import *
from simulator.util import *

from simulator.broker import BrokerSimulator
from simulator.node import AggNodeSimulator, ProcNodeSimulator
from simulator.cache import Cache, CacheIndex

# Initialize a grid
grid_arr = GridArray(2, 1, min_tasks=4, max_tasks=4)

# Initialize the Cache Index
ci = CacheIndex()
ci.start()

# Initialize the Broker Simulator/s
bsim_list = []
for g in grid_arr.items():
    bsim_list.append( BrokerSimulator(g) )

for bsim in bsim_list:
    bsim.start()

# Wait for everything to shut down
for bsim in bsim_list:
    bsim.join()

ci.join()

print("Press any key to shutdown...")
input()

