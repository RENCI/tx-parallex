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
    def __init__(self, mod, func, *args, task_id=None, **kwargs):
        super().__init__(DispatchMode.RANDOM)
        self.task_id = task_id if task_id is not None else str(uuid1())
        self.mod = mod
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def run(self, result):
        mod = import_module(self.mod)
        func = getattr(mod, self.func)
        return func(*self.args, **result, **self.kwargs)

        
class EndOfQueue(AbsTask):
    def __init__(self):
        super().__init__(DispatchMode.BROADCAST)

        
def dispatch(job_queue, worker_queues):
    n = len(worker_pipes)
    while True:
        jri = job_queue.get()
        job, result, jid = jri
        if job.dispatch_mode == DispatchMode.BROADCAST:
            for p in worker_queues:
                p.put(job)
            job_queue.complete(jid)
        elif job.dispatch_mode == DispatchMode.RANDOM:
            base = random(n)
            done = False
            for off in range(n):
                i = base + off
                if not worker_queues[i].full():
                    worker_queues[i].put(jri)
                    done = True
                    break
            if not done:
                worker_queues[base].put(jri)

    
def work_on(sub_queue):
    while True:
        job, result, jid = sub_queue.get()
        if isinstance(job, EndOfQueue):
            break
        else:
            resultj = job.run(result)
            sub_queue.complete(jid, resultj)
    

def generate_tasks(spec, data, top={}):
    if spec["type"] == "map":
        for row in data:
            yield from roundrobin(generate_tasks(subspec, row) for subspec in spec["sub"])
    elif spec["type"] == "top":
        subs = spec["sub"]
        subs_sorted = sort_tasks(subs)
        top = {}
        for sub in sub_sorted:
            generate_tasks(sub, data, top=top)
    elif spec["type"] == "python":
        name = spec["name"]
        mod = import_module(spec["mod"])
        func = getattr(module, spec["func"])
        dependencies = {k : top[v] for k, v in spec.get("depends_on", {}).items()}
        task = Task(mod, func)
        top[name] = task.task_id
        yield task, dependencies


def enqueue(spec, data, job_queue):
    for task, dependencies in generate_tasks(spec, data):
        job_queue.put(job, job_id=task.task_id, depends_on=dependencies)

    job_queue.put(EndOfQueue())
    
