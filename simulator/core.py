import random
import math
from multiprocessing import Queue, Event

class Grid():
    def __init__(self, x, y, node_count):
        self.x = x
        self.y = y
        self.node_count = node_count
        self.latency_map = None
        return
    
    def getLatency(self, to_x, to_y):
        if self.latency_map == None:
            print("Error: Latency map not initialized")
            return -1.0
    
        return self.latency_map.getLatency(to_x, to_y)

class GridArray():
    def __init__(self, size_x, size_y, min_tasks=2, max_tasks=3):
        self.array = []
        self.size_x = size_x
        self.size_y = size_y
        
        # Initialize grid array
        for x in range(0, size_x):
            row = []
            for y in range(0, size_y):
                g = Grid(x, y, random.randint(min_tasks, max_tasks))
                row.append(g)
            
            self.array.append(row)
        
        # Generate the latency maps for each grid
        for x in range(0, self.size_x):
            for y in range(0, self.size_y):
                g = self.array[x][y]
                g.latency_map = LatencyMap(g, list(self.items()))
                
        return
    
    def items(self):
        for x in range(0, self.size_x):
            for y in range(0, self.size_y):
                yield self.array[x][y]
    
    def getGrid(self, x, y):
        return self.array[x][y]
    
    def display(self):
        for row in self.array:
            print(row)
        
        return
    
class LatencyMap():
    def __init__(self, ref_grid, grid_array):
        self.grid = ref_grid
        self.latency_map = self.generate(grid_array)
        return

    def generate(self, grid_array):
        latency_map = []
        for other_grid in grid_array:
            if (other_grid.x == self.grid.x) and (other_grid.y == self.grid.y):
                # No latency for the origin
                latency_map.append({'x' : other_grid.x,
                                    'y' : other_grid.y,
                                    'latency' : 0.0})
                continue

            # If we can access the other latency map, then we simply
            #  draw data from that one instead of randomizing
            if (other_grid.latency_map != None):
                rel_latency = other_grid.getLatency(self.grid.x, self.grid.y)
                latency_map.append({'x' : other_grid.x,
                                    'y' : other_grid.y,
                                    'latency' : rel_latency})
                continue

            x_diff = abs(self.grid.x - other_grid.x)
            y_diff = abs(self.grid.y - other_grid.y)
            max_latency_value = math.sqrt((x_diff ** 2) + (y_diff ** 2)) * 2.0
            
            latency_map.append({'x' : other_grid.x,
                                'y' : other_grid.y,
                                'latency' : random.uniform(0, max_latency_value)})

        return latency_map
    
    def getLatency(self, to_x, to_y):
        for lm in self.latency_map:
            if (to_x == lm['x']) and (to_y == lm['y']):
                return lm['latency']
        
        return -1.0
    
    
class QueueManager():
    def __init__(self):
        self.queues = {}
        self.events = {}
        return
    
    def registerQueue(self, qid):
        self.queues[qid] = Queue()
        self.events[qid] = Event()
        return (self.queues[qid], self.events[qid])
    
    def getQueue(self, qid):
        return self.queues[qid]
    
    def getEvent(self, qid):
        return self.events[qid]
    
    def getQueueObjs(self, qid):
        return (self.queues[qid], self.events[qid])
    
    def unregisterQueue(self, qid):
        del self.queues[qid]
        del self.events[qid]
        return

# if __name__ == "__main__":
#     # Run supeficial tests
#     GA = GridArray(2,2)
#     ref_grid = GA.getGrid(0,0)
#     LM = LatencyMap(ref_grid, list(GA.items()))
    
#     latency = LM.getLatency(1,1)
#     print(latency)
    
#     qm = QueueManager()
#     qm.registerQueue('1234')
    
    
