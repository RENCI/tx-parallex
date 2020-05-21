from queue import Queue
from uuid import uuid4
from random import choice
from enum import Enum
from importlib import import_module
from more_itertools import roundrobin
from autorepr import autorepr, autotext
from multiprocessing import Manager
from ast import parse, Call, Name, UnaryOp, Constant, List, Dict, Return, For
import logging
from tx.functional.either import Left, Right, Either
from .dependentqueue import DependentQueue, SubQueue
from .utils import inverse_function
from .python import python_to_spec

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
        logger.info(f"self.task_id = {self.task_id}")
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
        logger.info(f"dispatching {jri}")
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
            try:
                resultv = {}
                error = False
                for k, v in result.items():
                    if isinstance(v, Left):
                        resultj = v
                        error = True
                        break
                    else:
                        resultv[k] = v.value
                if not error:                    
                    resultj = job.run(resultv)
                    if not isinstance(resultj, Either):
                        resultj = Right(resultj)
            except Exception as e:
                resultj = Left(str(e))
            sub_queue.complete(jid, resultj)
    

def get_task_depends_on(sub):
    if sub["type"] == "python":
        return {k: v["depends_on"] for k, v in sub.get("params", {}).items() if "depends_on" in v}
    elif sub["type"] == "map":
        if "depends_on" in sub["coll"]:
            return {sub["var"]: sub["coll"]["depends_on"]}
        else:
            return {}
    else:
        raise RuntimeError(f"get_task_depends_on: unsupported task {sub}")


def get_task_non_dependency_params(spec):
    return {k: v for k, v in spec.get("params", {}).items() if "depends_on" not in v}


def sort_tasks(subs):
    copy = list(subs)
    subs_sorted = []
    visited = set()
    while len(copy) > 0:
        copy2 = []
        updated = False
        for sub in copy:
            name = sub["name"]
            logger.info(f"name = {name}")
            depends_on = get_task_depends_on(sub)
            logger.info(f"depends_on = {depends_on}")
            if len(set(depends_on.values()) - visited) == 0:
                visited.add(name)
                subs_sorted.append(sub)
                updated = True
            else:
                copy2.append(sub)
        if updated:
            copy = copy2
        else:
            raise RuntimeError(f"unresolved dependencies or cycle in depedencies graph {list(map(lambda x:x['name']+str(x['params']), copy))}")

    print(subs_sorted)
    return subs_sorted


def arg_spec_to_arg(data, arg):
    logger.info(f"arg = {arg}")
    if "name" in arg:
        argnamereference = arg["name"]
        if not argnamereference in data:
            raise RuntimeError(f"undefined data {argnamereference}")
        return data[argnamereference]
    else:
        return arg["data"]


def generate_tasks(spec, data, top={}, ret_prefix=[]):
    ty = spec.get("type")
    if ty == "let":
        obj = spec["obj"]
        sub = spec["sub"]
        data2 = {**data, **obj}
        yield from generate_tasks(sub, data2, top={}, ret_prefix=ret_prefix)
    elif ty == "map":
        coll_name = spec["coll"]
        var = spec["var"]
        coll = arg_spec_to_arg(data, coll_name)
        subspec = spec["sub"]
        yield from roundrobin(*(generate_tasks(subspec, data2, top={}, ret_prefix=ret_prefix + [i]) for i, row in enumerate(coll) if (data2 := {**data, var:row})))
    elif ty == "top":
        subs = spec["sub"]
        subs_sorted = sort_tasks(subs)
        top = {}
        for sub in subs_sorted:
            yield from generate_tasks(sub, data, top=top, ret_prefix=ret_prefix)
    elif ty == "python":
        name = spec["name"]
        mod = spec["mod"]
        func = spec["func"]
        ret = spec.get("ret", [])
        fqret = list(map(lambda ret: ".".join(map(str, ret_prefix + [ret])), ret))
        params = get_task_non_dependency_params(spec)
        dependencies = {top[v]: ks for v, ks in inverse_function(get_task_depends_on(spec)).items()}
        if "task_id" in data:
            raise RuntimeError("task_id cannot be used as a field name")

        args0 = {k: arg_spec_to_arg(data, v) for k, v in params.items()}
        kwargs = {k: v for k, v in args0.items() if type(k) == str}
        args = list(map(lambda x: x[1], sorted({k: v for k, v in args0.items() if type(k) == int}.items(), key = lambda x: x[0])))
        task = Task(mod, func, *args, **kwargs)
        top[name] = task.task_id
        logger.info(f"add task: {task.task_id} depends_on {dependencies}")
        yield task, fqret, dependencies
    elif ty == "dsl":
        py = spec.get("python")
        spec2 = python_to_spec(py)
        yield from generate_tasks(spec2, data, top=top, ret_prefix=ret_prefix)
    else:
        raise RuntimeError(f'unsupported spec type {ty}')


def enqueue(spec, data, job_queue):
    job_ids = {}
    for what in generate_tasks(spec, data):
        job, ret, dependencies = what
        job_id = job.task_id
        job_queue.put(job, job_id=job_id, ret=ret, depends_on=dependencies)
        job_ids[job_id] = job_id

    job_queue.put(EndOfQueue(), depends_on={job_id: [] for job_id in job_ids})
    

        
        
    
