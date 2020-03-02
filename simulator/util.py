import random

class ShortId():
    def __init__(self, char_pool=list("ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890")):
        self.char_pool = char_pool
        return

    def generate(self, n_chars=6):
        return "".join(random.sample(self.char_pool, n_chars))

class QueryGenerator():
    def __init__(self, grids):
        self.grids = grids
        return
    
    def generate(self, n_queries=20, n_input_types=5, min_tasks=2, max_tasks=2):
        query_list = []
        sid_gen = ShortId()
        
        input_list = [ ShortId().generate(6) for _ in range(0,n_input_types) ]
        
        for i in range(0, n_queries):
            grid = random.choice(self.grids)
            
            query = { 
                'id'     : "Q-{}".format(sid_gen.generate()),
                'inputs' : random.choice(input_list),
                'grid'   : { 'x' : grid.x, 
                             'y' : grid.y },
                'tasks'  : { 'collection'  : 1,
                             'processing'  : random.randint(min_tasks, max_tasks),
                             'aggregation' : 1},
                'load'   : float(1 + (random.random() * random.randint(4, 8))),
            }
            
            query_list.append(query)
        
        return query_list
    
class AttrDict(dict):
    def __init__(self, *args, **kwargs):
        super(AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self
