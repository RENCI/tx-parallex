from queue import Queue
from uuid import uuid4
from random import choice
from enum import Enum
from importlib import import_module
from more_itertools import roundrobin
from autorepr import autorepr, autotext
from multiprocessing import Manager
from .dependentqueue import DependentQueue, SubQueue


class DispatchMode(Enum):
    RANDOM = 0
    BROADCAST = 1

    
class AbsTask:
    def __init__(self, dispatch_mode):
        self.dispatch_mode = dispatch_mode

        
class Task(AbsTask):
    def __init__(self, mod, func, *args, task_id=None, **kwargs):
        super().__init__(DispatchMode.RANDOM)
        self.task_id = task_id if task_id is not None else str(uuid4())
        print("self.task_id =", self.task_id)
        self.mod = mod
        self.func = func
        self.args = args
        self.kwargs = kwargs

    __repr__ = autorepr(["task_id", "mod", "func", "args", "kwargs"])
    __str__, __unicode__ = autotext("{self.task_id} {self.mod}.{self.func} {self.args} {self.kwargs}")

    def run(self, result):
        mod = import_module(self.mod)
        func = getattr(mod, self.func)
        return func(*self.args, **result, **self.kwargs)

        
class EndOfQueue(AbsTask):
    def __init__(self):
        super().__init__(DispatchMode.BROADCAST)

        
def dispatch(job_queue, worker_queues):
    n = len(worker_queues)
    while True:
        jri = job_queue.get()
        job, result, jid = jri
        print(f"dispatching {jri}")
        if job.dispatch_mode == DispatchMode.BROADCAST:
            for p in worker_queues:
                p.put(jri)
            job_queue.complete(jid)
        elif job.dispatch_mode == DispatchMode.RANDOM:
            base = choice(range(n))
            done = False
            for off in range(n):
                i = base + off
                if not worker_queues[i].full():
                    worker_queues[i].put(jri)
                    done = True
                    break
            if not done:
                worker_queues[base].put(jri)
        if isinstance(job, EndOfQueue):
            break

    
def work_on(sub_queue):
    while True:
        job, result, jid = sub_queue.get()
        if isinstance(job, EndOfQueue):
            break
        else:
            resultj = job.run(result)
            sub_queue.complete(jid, resultj)
    

def sort_tasks(subs):
    copy = list(subs)
    subs_sorted = []
    visited = set()
    while len(copy) > 0:
        copy2 = []
        updated = False
        for sub in copy:
            name = sub["name"]
            print("name =", name)
            depends_on = sub.get("depends_on", {})
            print("depends_on =", depends_on)
            if len(set(depends_on.keys()) - visited) == 0:
                visited.add(name)
                subs_sorted.append(sub)
                updated = True
            else:
                copy2.append(sub)
        if updated:
            copy = copy2
        else:
            raise RuntimeError("cycle in depedencies graph")

    print(subs_sorted)
    return subs_sorted


def generate_tasks(spec, data, top={}, ret_prefix=[]):
    if spec["type"] == "map":
        print("map")
        coll_name = spec["coll"]
        var = spec["var"]
        coll = data[coll_name]
        for i, row in enumerate(coll):
            data2 = {**data, var:row}
            yield from roundrobin(*(generate_tasks(subspec, data2, ret_prefix=ret_prefix + [i]) for subspec in spec["sub"]))
    elif spec["type"] == "top":
        print("top")
        subs = spec["sub"]
        subs_sorted = sort_tasks(subs)
        top = {}
        for sub in subs_sorted:
            yield from generate_tasks(sub, data, top=top, ret_prefix=ret_prefix)
    elif spec["type"] == "python":
        print("python")
        name = spec["name"]
        mod = spec["mod"]
        func = spec["func"]
        ret = spec.get("ret")
        if ret is not None:
            ret = ".".join(ret_prefix + [ret])
        params = spec.get("params", {})
        dependencies = {top[v]: ks for v, ks in spec.get("depends_on", {}).items()}
        if "task_id" in data:
            raise RuntimeError("task_id cannot be used as a field name")
        args = {v: data[k] for k, vs in params.items() for v in vs}
        task = Task(mod, func, **args)
        top[name] = task.task_id
        print("add task:", task.task_id, "depends_on", dependencies)
        yield task, ret, dependencies
    else:
        raise RuntimeError(f'unsupported spec type {spec["type"]}')


def enqueue(spec, data, job_queue):
    job_ids = {}
    for what in generate_tasks(spec, data):
        job, ret, dependencies = what
        job_id = job.task_id
        job_queue.put(job, job_id=job_id, ret=ret, depends_on=dependencies)
        job_ids[job_id] = job_id

    job_queue.put(EndOfQueue(), depends_on={job_id: [] for job_id in job_ids})
    

        
        
    
