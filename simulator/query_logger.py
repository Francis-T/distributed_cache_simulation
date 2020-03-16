import paho.mqtt.client as mqtt
import csv
import time
import json
import os
from multiprocessing import Process

class QueryCompletionLogger(Process):
    def __init__(self, debug_tags=""):
        Process.__init__(self)
        self.id = "TCL"
        self.client = mqtt.Client()
        self.client.on_connect = self.onConnectHandler
        self.client.on_message = self.onMessageHandler
        
        self.logger_id = "log_{}".format(int(time.time()))
        self.logger_file = "{}.csv".format(self.logger_id)
        
        self.debug_tags = debug_tags
        
        return
    
    # The callback for when the client receives a CONNACK response from the server.
    def onConnectHandler(self, client, userdata, flags, rc):
        print("Connected with result code "+str(rc))
        client.subscribe("all")
        client.subscribe("query_complete")
        return

    # The callback for when a PUBLISH message is received from the server.
    def onMessageHandler(self, client, userdata, msg):
        topic = str(msg.topic)
        content = json.loads(msg.payload)
        
#         self.verbose("Received {} from {}".format(topic, content))
        if content['type'] == 'shutdown':
            self.verbose("Shutting down...")
            client.disconnect()
            return
        
        elif content['type'] == 'completion_result':
            self.verbose("Handling Completion Result")
            row = [ content['query_id'], content['from_cache'], content['load'], 
                    content['cache_retrieval_delay'], content['count'], 
                    content['started'], content['ended'], (content['ended'] - content['started'])  ]
            
            header_required = False
            if not os.path.exists(self.logger_file):
                header_required = True
            
            with open(self.logger_file, 'a+', newline='') as csvfile:
                writer = csv.writer(csvfile, delimiter=',', quoting=csv.QUOTE_MINIMAL)
                
                if header_required:
                    writer.writerow(['query_id', 'from_cache', 'simulated_load', 'cache_retrieval_delay', 
                                     'task_count', 'start_time', 'end_time', 'duration'] )
                    
                writer.writerow(row)
                self.verbose("Result logged")
            
        
        return
    
    def run(self):
        self.client.connect("localhost", 1883, 60)
        self.client.loop_forever()
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

