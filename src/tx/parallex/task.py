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
import traceback
from tx.functional.either import Left, Right, Either
from .dependentqueue import DependentQueue, SubQueue
from .utils import inverse_function
from .python import python_to_spec
from .stack import Stack

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DispatchMode(Enum):
    RANDOM = 0
    BROADCAST = 1

    
class AbsTask:
    def __init__(self, dispatch_mode, task_id=None):
        self.dispatch_mode = dispatch_mode
        self.task_id = task_id if task_id is not None else str(uuid4())
        logger.info(f"AbsTask.__init__: self.task_id = {self.task_id}")


def substitute_list(results, args):
    return [results[arg] for arg in args if arg in results]

def substitute_dict(results, kwargs):
    return {k: results[v] for k, v in kwargs.items() if v in results}
    
class Task(AbsTask):
    def __init__(self, mod, func, args_spec, kwargs_spec, *args, task_id=None, **kwargs):
        super().__init__(DispatchMode.RANDOM, task_id=task_id)
        self.mod = mod
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.args_spec = args_spec
        self.kwargs_spec = kwargs_spec

    __repr__ = autorepr(["task_id", "mod", "func", "args_spec", "kwargs_spec", "args", "kwargs"])
    __str__, __unicode__ = autotext("{self.task_id} {self.mod}.{self.func} {self.args_spec} {self.kwargs_spec} {self.args} {self.kwargs}")

    def run(self, results, subnode_depends, subnode_results, queue):
        mod = import_module(self.mod)
        func = getattr(mod, self.func)
        args = substitute_list(results, self.args_spec)
        kwargs = substitute_dict(results, self.kwargs_spec)
        return func(*self.args, *args, **self.kwargs, **kwargs)

        
class Dynamic(AbsTask):
    def __init__(self, var, coll_spec, data_spec, spec, data, ret_prefix, task_id=None):
        super().__init__(DispatchMode.RANDOM, task_id=task_id)
        self.var = var
        self.coll_spec = coll_spec
        self.data_spec = data_spec
        self.spec = spec
        self.data = data
        self.ret_prefix = ret_prefix

    __repr__ = autorepr(["task_id", "var", "coll_spec", "data_spec", "spec", "data", "ret_prefix"])
    __str__, __unicode__ = autotext("{self.task_id} {self.var} {self.coll_spec} {self.data_spec} {self.spec} {self.data} {self.ret_prefix}")

    def run(self, results, subnode_depends, subnode_results, queue):
        enqueue({
            "type": "map",
            "var": self.var,
            "coll": {
                "data": results[self.coll_spec]
            },
            "sub": self.spec
        }, {**self.data, **substitute_dict(subnode_results, self.data_spec)}, queue, top=EnvStack(subnode_depends), ret_prefix=self.ret_prefix)

        
class EndOfQueue(AbsTask):
    def __init__(self):
        super().__init__(DispatchMode.RANDOM)

        
def dispatch(job_queue, worker_queues):
    n = len(worker_queues)
    while True:
        jri = job_queue.get()
        logger.info(f"dispatching {jri}")
        job, results, subnode_depends, subnode_results, jid = jri
        if isinstance(job, EndOfQueue):
            for p in worker_queues:
                p.put_in_subqueue(jri)
            break
        else:
            base = choice(range(n))
            done = False
            for off in range(n):
                i = base + off
                if not worker_queues[i].full():
                    worker_queues[i].put_in_subqueue(jri)
                    done = True
                    break
            if not done:
                worker_queues[base].put_in_subqueue(jri)

    
def work_on(sub_queue):
    while True:
        jri = sub_queue.get()
        job, results, subnode_depends, subnode_results, jid = jri
        if isinstance(job, EndOfQueue):
            break
        else:
            try:
                resultv = {}
                error = False
                for k, v in results.items():
                    if isinstance(v, Left):
                        resultj = v
                        error = True
                        break
                    else:
                        resultv[k] = v.value
                if not error:
                    resultj = job.run(resultv, subnode_depends, subnode_results, sub_queue)
                    if not isinstance(resultj, Either):
                        resultj = Right(resultj)
            except Exception as e:
                resultj = Left((str(e), traceback.format_exc()))
            sub_queue.complete(jid, resultj)
    

def get_python_task_depends_on(sub):
    if sub["type"] == "python":
        return {k: v["depends_on"] for k, v in sub.get("params", {}).items() if "depends_on" in v}
    else:
        raise RuntimeError(f"get_task_depends_on: unsupported task {sub}")


def get_task_depends_on(sub):
    if sub["type"] == "python":
        return {v["depends_on"] for _, v in sub.get("params", {}).items() if "depends_on" in v}
    elif sub["type"] == "map":
        dependencies = get_task_depends_on(sub["sub"])
        if "depends_on" in sub["coll"]:
            dependencies.add(sub["coll"]["depends_on"])
        return dependencies
    elif sub["type"] == "let":
        return get_task_depends_on(sub["sub"])
    elif sub["type"] == "top":
        return set.union(*map(get_task_depends_on, sub["sub"]))
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
            name = sub.get("name")
            depends_on = get_task_depends_on(sub)
            if len(depends_on - visited) == 0:
                if name is not None:
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


def split_args(args0):
    kwargs = {k: v for k, v in args0.items() if type(k) == str}
    args = list(map(lambda x: x[1], sorted({k: v for k, v in args0.items() if type(k) == int}.items(), key = lambda x: x[0])))
    return args, kwargs

def arg_spec_to_arg(data, arg):
    if "name" in arg:
        argnamereference = arg["name"]
        if not argnamereference in data:
            raise RuntimeError(f"undefined data {argnamereference}")
        return data[argnamereference]
    else:
        return arg["data"]


EnvStack = Stack({})

def generate_tasks(spec, data, top=EnvStack(), ret_prefix=[]):
    ty = spec.get("type")
    if ty == "let":
        obj = spec["obj"]
        sub = spec["sub"]
        data2 = {**data, **obj}
        yield from generate_tasks(sub, data2, top=EnvStack(top), ret_prefix=ret_prefix)
    elif ty == "map":
        coll_name = spec["coll"]
        var = spec["var"]
        subspec = spec["sub"]
        if "depends_on" in coll_name:
            # dynamic task
            coll_spec = top[coll_name["depends_on"]]
            subnode_dep = get_task_depends_on(subspec)
            data_spec = {name: top[name] for name in subnode_dep}
            task = Dynamic(var, coll_spec, data_spec, subspec, data, ret_prefix)
            dep = {coll_spec}
            logger.info(f"add task: {task.task_id} depends_on {dep} : {subnode_dep}")
            yield task, [], dep, subnode_dep 
        else:
            coll = arg_spec_to_arg(data, coll_name)
            yield from roundrobin(*(generate_tasks(subspec, data2, top=EnvStack(top), ret_prefix=ret_prefix + [i]) for i, row in enumerate(coll) if (data2 := {**data, var:row})))
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
        if "task_id" in data:
            raise RuntimeError("task_id cannot be used as a field name")

        args0 = {k: arg_spec_to_arg(data, v) for k, v in get_task_non_dependency_params(spec).items()}
        args, kwargs = split_args(args0)
        dependencies = {k: top[v] for k, v in get_python_task_depends_on(spec).items()}
        args_spec, kwargs_spec = split_args(dependencies)
        task = Task(mod, func, args_spec, kwargs_spec, *args, **kwargs)
        top[name] = task.task_id
        logger.info(f"add task: {task.task_id} depends_on {dependencies}")
        yield task, fqret, set(dependencies.values()), set()
    else:
        raise RuntimeError(f'unsupported spec type {ty}')


def enqueue(spec, data, job_queue, top=EnvStack(), ret_prefix=[]):
    job_ids = {}
    for what in generate_tasks(spec, data, top=top, ret_prefix=ret_prefix):
        job, ret, dependencies, subnode_dependencies = what
        job_id = job.task_id
        job_queue.put(job, job_id=job_id, ret=ret, depends_on=dependencies, subnode_depends_on=subnode_dependencies)
        job_ids[job_id] = job_id

    

        
        
    
