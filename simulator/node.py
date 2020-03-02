import paho.mqtt.client as mqtt

import time
import queue
import json

from simulator.core import QueueManager
from simulator.util import AttrDict, ShortId
from multiprocessing import Queue, Event, Process

class AggNodeSimulator(Process):
    def __init__(self, queue_info, active_queries, broker_sub, timeout=1, debug_tags="log|debug|verbose"):
        Process.__init__(self)
        self.id = "AGG-{}".format(ShortId().generate())
        
        # Set the request/response queue and event
        self.queue_info = queue_info
        # queue_info is an AttrDict with
        #    - queue_info.request.queue
        #    - queue_info.request.event
        #    - queue_info.response.queue
        #    - queue_info.response.event
        
        self.active_queries = active_queries

        self.client = mqtt.Client()
        self.broker_sub = broker_sub

        self.timeout = timeout
        
        self.debug_tags = debug_tags
        self.verbose("Started.")
        return
    
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
        self.verbose("Running.")
        sid_gen = ShortId()
        
        self.client.connect("localhost",1883,60)
        while True:
            self.verbose("Queue Info: {}".format(self.queue_info))
            self.queue_info.request.event.wait()
            
            request = None
            try:
                request = self.queue_info.request.queue.get(block=False, timeout=self.timeout)
            
            except queue.Empty:
                continue
                
            self.verbose("Request Received: {}".format(request))
            if request['type'] == 'shutdown':
                self.debug("Shutdown invoked")
                break
            
            elif request['type'] == 'status':
                response = {'node_id' : self.id,
                            'status' : 'done'}
                self.queue_info.response.queue.put(response)
                self.queue_info.response.event.set()
                
            elif request['type'] == 'task_response':
                q_id = request['query_id']

                # Decrement the task count for this query
                self.verbose("Task count: {}".format(self.active_queries[q_id]['count']))
                self.active_queries[q_id]['count'] = self.active_queries[q_id]['count'] - 1
                self.verbose("Tasks remain: {}".format(self.active_queries[q_id]['count']))
                
                if self.active_queries[q_id]['count'] <= 0:
                    finished_query = {
                        'type'    : 'aggregation_result',
                        'query_id': q_id,
                        'inputs'  : self.active_queries[q_id]['inputs'],
                        'grid'    : self.active_queries[q_id]['grid'],
                        'tasks'   : self.active_queries[q_id]['tasks'],
                        'load'    : self.active_queries[q_id]['load'],
                        'count'   : 0, 
                        'started' : self.active_queries[q_id]['started'],
                        'ended'   : time.time(),
                        'result'  : ShortId().generate(),
                    }

                    self.verbose("Aggregation Result: {}".format(finished_query))
                    self.verbose("Target: {}".format(self.broker_sub))
                    self.client.publish(self.broker_sub, json.dumps(finished_query))

                    self.verbose("{} aggregation finished".format(q_id))
            
            # If there are more items, then process them too
            if self.queue_info.request.queue.empty():
                self.queue_info.request.event.clear()
        
        self.verbose("Shutdown.")
        return

class ProcNodeSimulator(Process):
    def __init__(self, queue_info, timeout=1, debug_tags="log|debug|verbose"):
        Process.__init__(self)
        self.id = "PROC-{}".format(ShortId().generate())
        
        # Set the request/response queue and event
        self.queue_info = queue_info
        # queue_info is an AttrDict with
        #    - queue_info.request.queue
        #    - queue_info.request.event
        #    - queue_info.response.queue
        #    - queue_info.response.event
        
        self.timeout = timeout
        
        self.debug_tags = debug_tags
        self.verbose("Started.")
        return
    
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
        self.verbose("Running.")
        sid_gen = ShortId()
        
        while True:
            self.verbose("Queue Info: {}".format(self.queue_info))
            self.queue_info.request.event.wait()
            
            request = None
            try:
                request = self.queue_info.request.queue.get(block=False, timeout=self.timeout)
            
            except queue.Empty:
                self.queue_info.request.event.clear()
                continue
                
            self.verbose("Request Received: {}".format(request))
            if request['type'] == 'shutdown':
                self.debug("Shutdown invoked")
                break
            
            elif request['type'] == 'status':
                response = {'node_id' : self.id,
                            'status' : 'done'}
                self.queue_info.response.queue.put(response)
                self.queue_info.response.event.set()
                
            elif request['type'] == 'task':
                self.verbose("Simulating task execution through sleep ({} secs)".format(request['exec_time']))
                # Simulate task execution via sleep
                time.sleep(request['exec_time'])
                self.verbose("Task done")

                # Send a response
                response = {'task_id' : "T-{}".format(sid_gen.generate(10)), 
                            'type' : 'task_response',
                            'query_id' : request['query_id'], 
                            'inputs' : request['inputs'],
                            'status' : 'done'}
                self.queue_info.response.queue.put(response)
                self.queue_info.response.event.set()
            
            # If there are more items, then process them too
            if self.queue_info.request.queue.empty():
                self.queue_info.request.event.clear()
        
        self.verbose("Shutdown.")
        return

if __name__ == "__main__":
    print("""
    #############################################
    ### Test Code for ProcNodeSimulator Class ###
    ###   Change cell type to 'code' when     ###
    ###               necessary               ###
    #############################################
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

    pns = ProcNodeSimulator(qi)
    pns.start()

    # test_queue, test_event = qm.registerQueue('test_resp')
    # Get the response queue
    resp_queue, resp_ev = qm.getQueueObjs('1_resp')

    q, ev = qm.getQueueObjs('1_req')
    # Test the status request
    print("[--] Testing ProcNodeSimulator Status Request")
    request = {
        'type' : 'status',
    }
    q.put(request)
    ev.set()

    resp_ev.wait()
    response = resp_queue.get()
    print(response)
    assert 'status' in response.keys()
    print("[OK] ProcNodeSimulator Status Request")

    # Test the task request
    print("[--] Testing ProcNodeSimulator Task Request")
    request = {
        'type'      : 'task',
        'query_id'  : ShortId().generate(),
        'inputs'    : '1+1',
        'exec_time' : 1.5,
    }
    q.put(request)
    ev.set()

    resp_ev.wait()
    response = resp_queue.get()
    print(response)
    assert 'task_id' in response.keys()
    print("[OK] ProcNodeSimulator Task Request")

    # Test the shutdown request
    print("[--] Testing ProcNodeSimulator Shutdown Request")
    request = {
        'type' : 'shutdown',
    }
    q.put(request)
    ev.set()

    time.sleep(1.0)

    assert not pns.is_alive()
    print("[OK] ProcNodeSimulator Shutdown Request")


    
