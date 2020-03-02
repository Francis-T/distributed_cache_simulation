import time
import queue
import json

#from simulator.core import QueueManager
from simulator.util import AttrDict, ShortId
from multiprocessing import Queue, Event, Process, Manager

import paho.mqtt.client as mqtt

class Cache():
    def __init__(self, cache_limit=20):
        self.contents = {}
        self.cache_limit = cache_limit
        return
    
    def add(self, k, v):
        if self.count() >= self.cache_limit:
            return False
        
        self.contents[k] = v
        return True
    
    def remove(self, k):
        if not k in self.contents.keys():
            return False
        
        del self.contents[k]
        return
    
    def getItem(self, k):
        if not k in self.contents.keys():
            return None
        
        return self.contents[k]
    
    def count(self):
        return len(self.contents.keys())
    
    def getLimit(self):
        return cache_limit

class CacheIndex(Process):
    def __init__(self, timeout=1, debug_tags="log|debug|verbose"):
        Process.__init__(self)
        self.id = "CI-{}".format(ShortId().generate())
        
        # Instantiate MQTT client
        self.client = mqtt.Client()
        self.client.on_connect = self.onConnectHandler
        self.client.on_message = self.onMessageHandler

        # Initialize MQTT variables
        self.subs_list = [ "all", "cache_general", "cache_index_requests" ]

        # Instantiate a Manager object for variables that need to be multiprocessed
        self.ci_manager = Manager()
        self.cache_index = self.ci_manager.dict()
        
        self.timeout = timeout
        
        self.debug_tags = debug_tags
        self.verbose("Started.")
        self.status = "INITIALIZED"
        return

    def onConnectHandler(self, client, userdata, flags, rc):
        self.verbose("Connected.")
        
        # Subscribe to general MQTT topics and own topics
        client.subscribe([ (sub, 0) for sub in self.subs_list ])
        self.verbose("Subscribed to: {}".format(self.subs_list))
        
        # Announce connection to channel
        payload_intro = {
            'type' : 'announce',
            'id' : self.id, # TODO include other capabilities here too
        }
        self.verbose("Announcement payload: {}".format(json.dumps(payload_intro)))
        client.publish("all", json.dumps(payload_intro))
        self.verbose("Announcement made.")

        return
    
    def onMessageHandler(self, client, userdata, msg):
        topic = str(msg.topic)
        request = json.loads(msg.payload)
        
        self.verbose("Received from {}: {}".format(topic, request))
        if (request['type'] == 'shutdown') and (topic in self.subs_list):
            self.verbose("Shutting down...")

            client.disconnect()

        elif (request['type'] == 'status') and (topic in self.subs_list):
            rc = self.handleStatusRequest(request, client, topic)
            if rc != True:
                self.debug("Error Occurred during handling of status request!")
                client.disconnect()
            
        elif (request['type'] == 'add') and (topic == 'cache_index_requests'):
            rc = self.handleAddEntryRequest(request, client)
            if rc != True:
                self.debug("Error Occurred during handling of add entry request!")
                client.disconnect()
            
        elif (request['type'] == 'remove') and (topic == 'cache_index_requests'):
            rc = self.handleRemoveEntryRequest(request, client)
            if rc != True:
                self.debug("Error Occurred during handling of remove entry request!")
                client.disconnect()
            
        elif (request['type'] == 'get_cache_list') and (topic == 'cache_index_requests'):
            rc = self.handleGetCacheListRequest(request, client)
            if rc != True:
                self.debug("Error Occurred during handling of get cache list request!")
                client.disconnect()
                
        return
    
    def handleStatusRequest(self, request, client, topic):
        response = {'id' : self.id,
                    'status' : self.status}
        # Send the result
        client.publish(topic, json.dumps(response))

        return True

    def handleAddEntryRequest(self, request, client):
        entry_key = request['input_key']
        entry_val = request['broker_id']
        
        if not entry_key in self.cache_index.keys():
            self.cache_index[entry_key] = [entry_val]
            self.debug("Added new entry")
            
        elif not entry_val in self.cache_index[entry_key]:
            self.cache_index[entry_key].append(entry_val)
            self.debug("Added entry to list")
                    
        return True

    def handleRemoveEntryRequest(self, request, client):
        entry_key = request['input_key']
        entry_val = request['broker_id']
        
        # If the key exists and the target value is under it, then we can remove it
        if (entry_key in self.cache_index.keys()) and (entry_val in self.cache_index[entry_key]):
            self.cache_index[entry_key].remove(entry_val)

            if not self.cache_index[entry_key]:
                del self.cache_index[entry_key]

        return True

    def handleGetCacheListRequest(self, request, client):
        self.verbose("Processing Get Cache List Request")
        entry_key = request['input_key']

        cache_list = []
        if entry_key in self.cache_index.keys():
            cache_list = self.cache_index[entry_key]
        
        response = {
            'query_id' : request['query_id'],
            'type' : 'cache_index_response',
            'target_id' : request['broker_sub_id'],
            'input_key' : entry_key,
            'cache_list' : cache_list,
        }
        self.verbose("Cache List: {}".format(cache_list))
        client.publish(request['broker_sub_id'], json.dumps(response))
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
    
    def run(self):
        self.verbose("Connecting...")
        self.client.connect("localhost", 1883, 60)
        self.client.loop_forever()
        self.verbose("Shutdown.")
        return
    
    # def run(self):
    #     self.verbose("Running")
    #     self.status = "IDLE"
    #     while True:
    #         self.verbose("Awaiting Requests...")
    #         self.queue_info.request.event.wait()
    #         self.status = "BUSY"
    #         
    #         request = None
    #         try:
    #             request = self.queue_info.request.queue.get(block=False, timeout=self.timeout)
    #         
    #         except queue.Empty:
    #             continue
    #             
    #         self.verbose("Request received: {}".format(request))
    #         if request['type'] == 'shutdown':
    #             self.debug("Shutdown invoked")
    #             break
    #             
    #         elif request['type'] == 'add':
    #             entry_key = request['input_key']
    #             entry_val = request['broker_id']
    #             
    #             if not entry_key in self.cache_index.keys():
    #                 self.cache_index[entry_key] = [entry_val]
    #                 self.debug("Added new entry")
    #                 
    #             elif not entry_val in self.cache_index[entry_key]:
    #                 self.cache_index[entry_key].append(entry_val)
    #                 self.debug("Added entry to list")
    #                 
    #         elif request['type'] == 'remove':
    #             entry_key = request['input_key']
    #             entry_val = request['broker_id']
    #             
    #             # If the key exists and the target value is under it, then we can remove it
    #             if (entry_key in self.cache_index.keys()) and (entry_val in self.cache_index[entry_key]):
    #                 self.cache_index[entry_key].remove(entry_val)

    #                 if not self.cache_index[entry_key]:
    #                     del self.cache_index[entry_key]
    #         
    #         elif request['type'] == 'get_cache_list':
    #             entry_key = request['input_key']
    #             resp_queue_id = request['resp_queue_id']
    #             
    #             self.debug("QM Queues: {}".format(self.queue_manager.queues))
    #             
    #             resp_queue, resp_ev = self.queue_manager.getQueueObjs(resp_queue_id)
    #             
    #             response = {
    #                 'type' : 'cache_response',
    #                 'key' : entry_key,
    #                 'cache_list' : self.cache_index[entry_key]
    #             }
    #             resp_queue.put(response)
    #             resp_ev.set()
    #             
    #         elif request['type'] == 'status':
    #             resp_queue_id = request['resp_queue_id']
    #             
    #             resp_queue, resp_ev = self.queue_manager.getQueueObjs(resp_queue_id)
    #             
    #             response = {'id' : self.id,
    #                         'status' : self.status}
    #             resp_queue.put(response)
    #             resp_ev.set()
    #         
    #         # If there are more items, then process them too
    #         if self.queue_info.request.queue.empty():
    #             self.queue_info.request.event.clear()
    #     
    #     self.verbose("Shutdown.")
    #     self.status = "SHUTDOWN"
    #     return

if __name__ == "__main__":
    print("""
    ######################################
    ### Test Code for CacheIndex Class ###
    ###   Change cell type to 'code'   ###
    ###        when necessary          ###
    ######################################
    """)

    qm = QueueManager()

    q, ev = qm.registerQueue('1_req')
    qi = AttrDict()
    qi.request = AttrDict()
    qi.request.queue = q
    qi.request.event = ev

    q, ev = qm.registerQueue('1_resp')
    qi.response = AttrDict()
    qi.response.queue = q
    qi.response.event = ev

    test_queue, test_event = qm.registerQueue('test_resp')

    ci = CacheIndex(qi, qm)

    ci.start()

    # Prepare to access the cache index
    q, ev = qm.getQueueObjs('1_req')

    # Test add to cache index
    response = None
    print("[--] Testing CacheIndex Add Request")
    request = {
        'type' : 'add',
        'input_key' : '1+1',
        'broker_id' : '17171435'
    }
    q.put(request)

    request = {
        'type' : 'add',
        'input_key' : '1+1',
        'broker_id' : '3423894'
    }
    q.put(request)

    request = {
        'type' : 'add',
        'input_key' : '2+2',
        'broker_id' : '3423894'
    }
    q.put(request)
    ev.set()

    request = {
        'type' : 'get_cache_list',
        'input_key' : '1+1',
        'resp_queue_id' : 'test_resp',
    }
    q.put(request)
    ev.set()

    response = test_queue.get()
    assert len(response['cache_list']) == 2
    print("[OK] CacheIndex Add Request")

    # Test get from cache index
    print("[--] Testing CacheIndex Get Request")
    request = {
        'type' : 'get_cache_list',
        'input_key' : '2+2',
        'resp_queue_id' : 'test_resp',
    }
    q.put(request)
    ev.set()

    response = test_queue.get()
    assert len(response['cache_list']) == 1
    print("[OK] Cache Index Get Request")
    response = None

    # Test remove from cache index
    print("[--] Testing CacheIndex Remove Request")
    request = {
        'type' : 'remove',
        'input_key' : '1+1',
        'broker_id' : '17171435'
    }
    q.put(request)

    request = {
        'type' : 'get_cache_list',
        'input_key' : '1+1',
        'resp_queue_id' : 'test_resp',
    }
    q.put(request)
    ev.set()

    response = test_queue.get()
    assert len(response['cache_list']) == 1
    print("[OK] CacheIndex Remove Request")
    response = None

    # Test status from cache index
    print("[--] Testing CacheIndex Status Request")
    request = {
        'type' : 'status',
        'resp_queue_id' : 'test_resp',
    }
    q.put(request)
    ev.set()

    response = test_queue.get()
    print(response)
    assert 'status' in response.keys()
    print("[OK] CacheIndex Status Request")
    response = None

    # Test the shutdown request
    print("[--] Testing CacheIndex Shutdown Request")
    request = {
        'type' : 'shutdown',
    }
    q.put(request)
    ev.set()

    time.sleep(1.0)

    assert not ci.is_alive()
    print("[OK] CacheIndex Shutdown Request")
