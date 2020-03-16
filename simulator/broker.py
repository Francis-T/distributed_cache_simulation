import paho.mqtt.client as mqtt
import json
import time

from multiprocessing import Process, Queue, Event, Manager
from multiprocessing.managers import SyncManager

from simulator.core import *
from simulator.util import *
from simulator.cache import CacheIndex, Cache
from simulator.node import ProcNodeSimulator, AggNodeSimulator

QUERY_COMPLETION_SUB = "query_complete"

class BrokerSimulator(Process):
    def __init__(self, grid_info, timeout=5, debug_tags="log|debug|verbose", enable_caching=True):
        Process.__init__(self)
        self.debug_tags = debug_tags
        self.id = "B-{}".format(ShortId().generate())
        
        # Store the grid info for this broker
        self.grid = grid_info
        
        # Instantiate MQTT client
        self.verbose("Instantiating MQTT client")
        self.client = mqtt.Client()
        self.client.on_connect = self.onConnectHandler
        self.client.on_message = self.onMessageHandler

        # Initialize MQTT variables
        self.verbose("Initializing MQTT variables")
        self.broker_sub = "broker_{}".format(self.id)
        self.cache_sub = "cache_{}".format(self.id)
        self.subs_list = [ "all", "broker_general", "cache_general", "cache_index",
                           self.broker_sub, self.cache_sub ]
        
        # Instantiate a Manager object for variables that need to be multiprocessed
        self.verbose("Instantiating Manager object")
        self.broker_manager = Manager()
        #self.broker_manager.start()
        #self.broker_manager.start()

        self.active_queries = self.broker_manager.dict()
        self.finished_queries = self.broker_manager.dict()

        # Instantiate Task Queue objects
        self.verbose("Instantiating Task Queue objects")
        self.tq_info = AttrDict()
        self.tq_info.request = AttrDict()
        self.tq_info.response = AttrDict()
        self.tq_info.request.queue = self.broker_manager.Queue()
        self.tq_info.request.event = self.broker_manager.Event()
        self.tq_info.response.queue = self.broker_manager.Queue()
        self.tq_info.response.event = self.broker_manager.Event()

        self.cache = Cache()
        
        self.proc_nodes = []
        self.agg_nodes = []
        
        self.timeout = timeout
        self.status  = "INITIALIZED"
        self.caching_enabled = enable_caching

        self.verbose("Started.")
        return
    
    def run(self):
        self.verbose("Connecting...")
        self.client.connect("localhost", 1883, 60)
        self.client.loop_forever()
        # self.broker_manager.shutdown()
        self.verbose("Shutdown.")
        return
    
    def onConnectHandler(self, client, userdata, flags, rc):
        self.verbose("Connected.")
        
        def launchNodes():
            self.verbose("Starting processing node/s")
            # Start the task processing nodes
            self.tq_info.request.event.clear()
            for i in range(0, self.grid.node_count):
                self.proc_nodes.append(ProcNodeSimulator(self.tq_info, self.broker_sub))
                self.proc_nodes[i].start()

            self.verbose("Processing nodes started.")

            # Start the aggregation node/s
            self.tq_info.response.event.clear()
            agg_info = AttrDict()
            agg_info.request = AttrDict()
            agg_info.request.queue = self.tq_info.response.queue
            agg_info.request.event = self.tq_info.response.event
            self.agg_nodes.append( AggNodeSimulator( agg_info, 
                                                     self.active_queries, 
                                                     self.broker_sub) )
            self.verbose("Starting aggregation node/s")
            self.agg_nodes[0].start()

            self.verbose("Aggregation nodes started.")
            return

        self.verbose("Launching Nodes in a separate process")
        Process(target=launchNodes).start()
        
        # Subscribe to general MQTT topics and own topics
        client.subscribe([ (sub, 0) for sub in self.subs_list ])
        self.verbose("Subscribed to: {}".format(self.subs_list))
        
        # Announce connection to channel
        payload_intro = {
            'type' : 'announce',
            'class': 'broker',
            'id'   : self.id, # TODO include other capabilities here too
            'grid' : { 'x' : self.grid.x, 'y' : self.grid.y, 'cache_limit' : self.cache.cache_limit, 'latency_map' : self.grid.latency_map.latency_map }
        }
        self.verbose("Announcement payload: {}".format(json.dumps(payload_intro)))
        client.publish("all", json.dumps(payload_intro))
        self.verbose("Announcement made.")

        return
    
    def onMessageHandler(self, client, userdata, msg):
        topic = str(msg.topic)
        request = json.loads(msg.payload)
        
        #self.verbose("Received from {}: {}".format(topic, request))
        if (request['type'] == 'shutdown') and (topic in ['all', 'broker_general', self.broker_sub]):
            self.verbose("Shutting down...")

            shutdown_request = { 'type' : 'shutdown' }

            self.verbose("Shutting down processing nodes...")
            for i in range(0, len(self.proc_nodes)):
                self.tq_info.request.queue.put(shutdown_request)

            self.tq_info.request.event.set()
        
            self.verbose("Waiting for processing nodes to shutdown...")
            for pn in self.proc_nodes:
                pn.join()

            self.verbose("Shutting down aggregation nodes...")
            self.tq_info.response.queue.put(shutdown_request)
            self.tq_info.response.event.set()

            self.verbose("Waiting for aggregation nodes to shutdown...")
            for an in self.agg_nodes:
                an.join()

            client.disconnect()

        elif (request['type'] == 'status') and (topic in ['all', 'broker_general', self.broker_sub]):
            rc = self.handleStatusRequest(request, client, topic)
            if rc != True:
                self.debug("Error Occurred during handling of status request!")
                client.disconnect()
            
        elif (request['type'] == 'query') and (topic == self.broker_sub):
            rc = self.handleQueryRequest(request, client)
            if rc != True:
                self.debug("Error Occurred during handling of query request!")
                client.disconnect()
                
        elif (request['type'] == 'get_cached_item') and (topic == self.cache_sub):
            rc = self.handleGetCachedItemRequest(request, client)
            if rc != True:
                self.debug("Error Occurred during handling of query request!")
                client.disconnect()
                
        elif (request['type'] == 'cache_index_response') and (topic == self.broker_sub):
            rc = self.handleQueryCacheIndexResponse(request, client)
            if rc != True:
                self.debug("Error Occurred during handling of cache index response!")
                client.disconnect()
                
        elif (request['type'] == 'get_cached_item_response') and (topic == self.broker_sub):
            rc = self.handleQueryCachedItemResponse(request, client)
            if rc != True:
                self.debug("Error Occurred during handling of cache response!")
                client.disconnect()
        
        elif (request['type'] == 'aggregation_result') and (topic == self.broker_sub):
            rc = self.handleAggregationResult(request, client)
            if rc != True:
                self.debug("Error Occurred during handling of aggregation result!")
                client.disconnect()

        elif (request['type'] == 'cache_reassign') and (topic in ['all', 'broker_general', self.broker_sub]):
            rc = self.handleCacheReassign(request, client)
            if rc != True:
                self.debug("Error Occurred during handling of cache reassign request!")
                client.disconnect()
        
        
        return
    
    def handleQueryRequest(self, query, client):
        self.verbose("Handling Query Request")
        # Save information about the pending query
        self.active_queries[query['id']] = self.broker_manager.dict({
                                                'inputs'  : query['inputs'],
                                                'grid'    : query['grid'],
                                                'tasks'   : query['tasks'],
                                                'load'    : query['load'],
                                                'count'   : query['tasks']['processing'], 
                                                'started' : time.time(),
                                                'ended'   : None, 
                                            })

        if not self.caching_enabled:
            self.verbose("Caching disabled. Switching over to full processing...")
            query['query_id'] = query['id']
            return self.handleQueryProcessing(query, client)
        
        # Check if this query's result can be retrieved from some other cache
        request = {
            'query_id' : query['id'],
            'type' : 'get_cache_list',
            'input_key' : query['inputs'],
            'broker_sub_id' : self.broker_sub,
        }
        self.verbose("Sending Cache Index Request: {}".format(request))
        client.publish("cache_index_requests", json.dumps(request))
        
        return True
    
    def handleQueryCacheIndexResponse(self, query, client):
        self.verbose("Handling Query Cache Index Response")
        # Check if this does not have an active query -- if so, disregard it
        if not query['query_id'] in self.active_queries.keys():
            return False
        
#         [ Expected CacheIndex Response ]
#
#             response = {
#                 'query_id' : query['id'],
#                 'type' : 'cache_index_response',
#                 'target_id' : broker id of requestor,
#                 'input_key' : key requested,
#                 'cache_list' : list of known holders of this info.
#             }
#       
        # If the returned list is empty, then we have to process this on our own
        if not query['cache_list']:
            self.verbose("Not in cache. Switching over to full processing...")
            return self.handleQueryProcessing(query, client)
        
        # Otherwise, attempt to retrieve the result from another cache
        target_id = random.choice(query['cache_list'])
        target_sub = "cache_{}".format(target_id)
        
        # Reload the input key from the list of known active queries
        input_key = self.active_queries[query['query_id']]['inputs']
        
        # Request the result from another cache
        request = {
            'query_id' : query['query_id'],
            'type' : 'get_cached_item',
            'resp_sub' : self.broker_sub,
            'input_key' : input_key,
        }
        self.verbose("Requesting cached item from {}...".format(target_sub))
        client.publish(target_sub, json.dumps(request))
        
        return True
    
    def handleGetCachedItemRequest(self, query, client):
        self.verbose("Handling Get Cached Item Request")
        # Context:  This is a standalone request for the value of an item
        #    that is currently cache in this broker. Usually, this is done 
        #    after getting confirmation from the cache index of the cached
        #    result's location.
        
#         [ Expected Broker Request ]
#
#         request = {
#             'query_id' : query['id'],
#             'type' : 'get_cached_item',
#             'resp_sub' : subscription topic of the response,
#             'input_key' : key of result to be retrieved from the cache,
#         }
#
        # Load the result from the cache
        cached_result = self.cache.getItem(query['input_key'])
        
        # Send the result
        target_sub = query['resp_sub']
        response = {
            'query_id'  : query['query_id'],
            'type'      : 'get_cached_item_response',
            'cache_sub' : self.cache_sub,
            'grid'      : {'x' : self.grid.x, 'y' : self.grid.y},
            'input_key' : query['input_key'],
            'result'    : cached_result,
        }
        client.publish(target_sub, json.dumps(response))
        return True
    
    def handleQueryCachedItemResponse(self, query, client):
        self.verbose("Handling Get Cached Item Response")
        # Context:  A cached item was previously requested from a target broker.
        #    In this function, we process the response of that broker and try
        #    to retrieve the value of the cached item from it
        
        # Check if this does not have an active query -- if so, disregard it
        q_id = query['query_id']
        if not q_id in self.active_queries.keys():
            return self.handleQueryProcessing(query, client)
        
#         [ Expected Broker Response ]
#
#         response = {
#             'query_id' : query['id'],
#             'type' : 'get_cached_item_response',
#             'cache_sub' : self.cache_sub,
#             'grid'      : {'x' : self.grid.x, 'y' : self.grid.y},
#             'input_key' : input_key,
#             'result'    : cached_result,
#         }
#
        # If nothing was found, then we handover to normal processing
        if query['result'] == None:
            # TODO Should this incur any cache retrieval penalties?
            return False
        
        # Otherwise, get the latency between the current grid and the cached item source
        targ_x = query['grid']['x']
        targ_y = query['grid']['y']
        
        cache_retrieval_delay = self.grid.getLatency(targ_x, targ_y)
        
        def delayed_finish_query():
            # Simulate the delay with sleep
            if cache_retrieval_delay > 0.0:
                time.sleep(cache_retrieval_delay)

            # Reload the ongoing query's information
            query_info = self.active_queries[q_id]
             
            # Add the aggregation result to the finished results
            self.verbose("Publishing finished result")
            finished_result = {
                    'type'    : 'completion_result',
                    'assigned': { 'x' : targ_x, 'y' : targ_y },
                    'origin'  : query_info['grid'],
                    'query_id': q_id,
                    'inputs'  : query_info['inputs'],
                    'tasks'   : query_info['tasks'],
                    'load'    : query_info['load'],
                    'count'   : query_info['count'], 
                    'started' : query_info['started'],
                    'ended'   : time.time(), 
                    'result'  : query['result'],
                    'from_cache' : True,
                    'cache_retrieval_delay' : cache_retrieval_delay,
            }
            query_client = mqtt.Client()
            query_client.connect("localhost", 1883, 60)
            query_client.publish(QUERY_COMPLETION_SUB, json.dumps(finished_result))

            # Remove query id from active queries
            del self.active_queries[q_id]

            self.verbose("Result found after {} secs: {}".format(cache_retrieval_delay, query['result']))
            query_client.disconnect()
            return

        Process(target=delayed_finish_query).start()

        return True
    
        
        # If not, then process it as normal
        #    Make a note of the active tasks
        #    Push the tasks to the task queue and set the event flag for it
        
    def handleQueryProcessing(self, query, client):
        self.verbose("Handling Query Processing")
        # Check if this does not have an active query -- if so, disregard it
        if not query['query_id'] in self.active_queries.keys():
            return False
            
        # Reload the ongoing query's information
        query_info = self.active_queries[query['query_id']]
        
        # Start a number of tasks by putting them on the task queue
        self.verbose("Starting tasks for query processing")
        self.verbose("    Query: {}".format(query))
        self.verbose("    Query Info: {}".format(query_info))

        task_count = query_info['count']
        for i in range(0, task_count):
            task_inputs = {
                'type'      : 'task',
                'query_id'  : query['query_id'],
                'exec_time' : query_info['load'],
                'inputs'    : query_info['inputs'],
            }
            self.tq_info.request.queue.put(task_inputs)
        
        # Tell the processing tasks to start
        self.tq_info.request.event.set()
        self.verbose("Tasks started")

        return True

    def handleAggregationResult(self, query, client):
        self.verbose("Handling Aggregation Result")
        # Check if this does not have an active query -- if so, disregard it
        q_id = query['query_id']
        if not q_id in self.active_queries.keys():
            return False

        # Add the aggregation result to the finished results
        self.verbose("Publishing finished result")
        finished_result = {
                'type'    : 'completion_result',
                'assigned': { 'x' : self.grid.x, 'y' : self.grid.y },
                'origin'  : { 'x' : self.grid.x, 'y' : self.grid.y },
                'query_id': q_id,
                'inputs'  : query['inputs'],
                'grid'    : query['grid'],
                'tasks'   : query['tasks'],
                'load'    : query['load'],
                'count'   : query['tasks']['processing'], 
                'started' : query['started'],
                'ended'   : query['ended'], 
                'result'  : query['result'],
                'from_cache' : False,
                'cache_retrieval_delay' : 0.0,
        }

        client.publish(QUERY_COMPLETION_SUB, json.dumps(finished_result))

        # Remove query id from active queries
        del self.active_queries[q_id]

        # Add the *result* to the cache
        self.cache.add( query['inputs'],  query['result'])

        # Tell the cache index to add a new entry as well
        request = {
            'type' : 'add',
            'input_key' : query['inputs'],
            'broker_id' : self.id,
        }
        self.verbose("Sending Cache Index Request: {}".format(request))
        client.publish("cache_index_requests", json.dumps(request))

        return True

    def handleCacheReassign(self, request, client, topic):
        # TODO
        nqa_map = request['map']

        # Cycle through each input key in the reassigned query map
        # for k in self.cache.getKeys():
        #     # Check if the input key is in our cache, if so 
        #     cached_item  = self.cache.getItem(k)
            

        return True
    
    def handleStatusRequest(self, request, client, topic):
        response = {'id' : self.id,
                    'status' : self.status}
        # Send the result
        client.publish(topic, json.dumps(response))

        return True
    
    def log(self, message):
        print("[{}] {}".format(self.id, message))
        return
    
    def verbose(self, message):
        if "verbose" in self.debug_tags:
            self.log(message)
        return
    
    def debug(self, message):
        if "debug" in self.debug_tags:
            self.log(message)
        return

