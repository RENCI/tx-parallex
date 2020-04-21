from queue import Queue
from uuid import uuid1
from random import choice
from enum import Enum
from more_itertools import roundrobin


class DispatchMode(Enum):
    RANDOM = 0
    BROADCAST = 1

    
class AbsTask:
    def __init__(self, dispatch_mode):
        self.dispatch_mode = dispatch_mode

        
class Task(AbsTask):
    def __init__(self, func, *args, **kwargs):
        super().__init__(DispatchMode.RANDOM)
        self.task_id = uuid1()
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def run(self):
        self.func(*self.args, **self.kwargs)

        
class EndOfQueue(AbsTask):
    def __init__(self):
        super().__init__(DispatchMode.BROADCAST)

        
def dispatch(job_queue, worker_queues):
    n = len(worker_pipes)
    while True:
        job = job_queue.get()
        if job.dispatch_mode == DispatchMode.BROADCAST:
            for p in worker_queues:
                p.put(job)
        elif job.dispatch_mode == DispatchMode.RANDOM:
            base = random(n)
            done = False
            for off in range(n):
                i = base + off
                if not worker_queues[i].full():
                    worker_queues[i].put(job)
                    done = True
                    break
            if not done:
                worker_queues[base].put(job)

    
def work_on(job_queue):
    while True:
        job = job_queue.get()
        if isinstance(job, EndOfQueue):
            break
        else:
            job.run()
    

def generate_tasks(spec, data):
    if spec["type"] == "map":
        for row in data:
            yield from roundrobin(generate_tasks(subspec, row) for subspec in spec["sub"])
    elif spec["type"] == "top":
        subs = spec["sub"]
        
    elif spec["type"] == "python":
        mod = import_module(spec["mod"])
        func = getattr(module, spec["func"])
        
        


def enqueue(spec, data, job_queue):
    for job in generate_tasks(spec, data):
        job_queue.put(job)

    job_queue.put(EndOfQueue())
    
