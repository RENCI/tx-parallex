import sys
from queue import Queue
from uuid import uuid4
from random import choice
from enum import Enum
from importlib import import_module
from itertools import chain
from more_itertools import roundrobin
from autorepr import autorepr, autotext
from ast import parse, Call, Name, UnaryOp, Constant, List, Dict, Return, For
import logging
import traceback
from graph import Graph
from functools import partial
from copy import deepcopy
from ctypes import c_int
import builtins
from tx.functional.either import Left, Right, Either
from tx.functional.maybe import Just, Nothing, maybe
from .dependentqueue import DependentQueue
from .utils import inverse_function
from .python import python_to_spec, EnvStack2
from .stack import Stack
import jsonpickle
from tx.readable_log import format_message, getLogger

logger = getLogger(__name__, logging.INFO)

def substitute_list(results, args):
    return [results[arg] for arg in args if arg in results]


def substitute_dict(results, kwargs):
    return {k: results[v] for k, v in kwargs.items() if v in results}
    

class AbsTask:
    def __init__(self, task_id=None):
        self.task_id = task_id if task_id is not None else str(type(self)) + "@" + str(next_task())
#        logger.debug(f"AbsTask.__init__: self.task_id = {self.task_id}")


class BaseTask(AbsTask):
    def run(self, results, subnode_results, queue):
        logger.debug("BaseTask.run: restuls = %s", results)
        return mbind(self.baseRun, results, subnode_results, queue)
    
# task_counter = Value(c_int, 0)

# max_task = 65536

def next_task():
    return uuid4()
    # with task_counter.get_lock():
    #     task = task_counter.value
    #     task += 1
    #     if task == max_task:
    #         task = 0
    #     task_counter.value = task
        
    #     return task



class Task(BaseTask):
    def __init__(self, name, mod, func, args_spec, kwargs_spec, args, kwargs, task_id=None):
        super().__init__(task_id=name + "@" + str(next_task()))
        self.name = name
        self.mod = mod
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.args_spec = args_spec
        self.kwargs_spec = kwargs_spec

    __repr__ = autorepr(["task_id", "name", "mod", "func", "args_spec", "kwargs_spec", "args", "kwargs"])
    __str__, __unicode__ = autotext("Task({self.task_id} {self.name} {self.mod}.{self.func} {self.args_spec} {self.kwargs_spec} {self.args} {self.kwargs})")

    def baseRun(self, results, subnode_results, queue):
        mod = import_module(self.mod) if self.mod != "" else builtins
        func = getattr(mod, self.func)
        args = substitute_dict(results, self.args_spec)
        kwargs = substitute_dict(results, self.kwargs_spec)
        self_arg_items = self.args.items()
        arg_items = args.items()
        return {}, func(*[v for _, v in sorted(chain(self_arg_items, arg_items), key=lambda x: x[0])], **self.kwargs, **kwargs)


class Hold(AbsTask):
    pass


class DynamicMap(BaseTask):
    def __init__(self, var, coll_spec, spec, data, subnode_top, ret_prefix, task_id=None):
        super().__init__(task_id=task_id)
        self.var = var
        self.coll_spec = coll_spec
        self.spec = spec
        self.data = data
        self.subnode_top = subnode_top
        self.ret_prefix = ret_prefix

    __repr__ = autorepr(["task_id", "var", "coll_spec", "spec", "data", "subnode_top", "ret_prefix"])
    __str__, __unicode__ = autotext("DynamicMap({self.task_id} {self.var} {self.coll_spec} {self.spec} {self.data} {self.subnode_top} {self.ret_prefix})")

    def baseRun(self, results, subnode_results, queue):
        hold_id = queue.put(Hold(), is_hold=True)
        logger.debug("DynamicMap.baseRun: put hold task on queue %s", hold_id)
        enqueue({
            "type": "map",
            "var": self.var,
            "coll": {
                "data": results[self.coll_spec]
            },
            "sub": self.spec
        }, {**self.data, **{name: subnode_results[node_id] for name, node_id in self.subnode_top.items()}}, queue, top=EnvStack(), ret_prefix=self.ret_prefix, execute_unreachable=True, hold={hold_id})
        queue.complete(hold_id, {}, Nothing)
        logger.debug("DynamicMap.baseRun: remove hold task from queue %s", hold_id)
        return {}, None

        
class DynamicGuard(BaseTask):
    def __init__(self, cond_spec, then_spec, else_spec, data, subnode_top, ret_prefix, task_id=None):
        super().__init__(task_id=task_id)
        self.cond_spec = cond_spec
        self.then_spec = then_spec
        self.else_spec = else_spec
        self.data = data
        self.subnode_top = subnode_top
        self.ret_prefix = ret_prefix

    __repr__ = autorepr(["task_id", "cond_spec", "then_spec", "else_spec", "data", "subnode_top", "ret_prefix"])
    __str__, __unicode__ = autotext("DynamicCond({self.task_id} {self.cond_spec} {self.then_spec} {self.else_spec} {self.data} {self.subnode_top} {self.ret_prefix})")

    def baseRun(self, results, subnode_results, queue):
        logger.debug("DynamicCond.baseRun: before hold")
        hold_id = queue.put(Hold(), is_hold=True)
        logger.debug("DynamicCond.baseRun: put hold task on queue %s", hold_id)
        enqueue({
            "type": "cond",
            "on": {
                "data": results[self.cond_spec]
            },
            "then": self.then_spec,
            "else": self.else_spec
        }, {**self.data, **{name: subnode_results[node_id] for name, node_id in self.subnode_top.items()}}, queue, top=EnvStack(), ret_prefix=self.ret_prefix, execute_unreachable=True, hold={hold_id})
        queue.complete(hold_id, {}, Nothing)
        logger.debug("DynamicCond.baseRun: remove hold task from queue %s", hold_id)
        return {}, None
    
        
class DynamicRet(AbsTask):
    def __init__(self, obj_spec, ret_prefix, task_id=None):
        super().__init__(task_id=task_id)
        self.obj_spec = obj_spec
        self.ret_prefix = ret_prefix

    __repr__ = autorepr(["task_id", "obj_spec", "ret_prefix"])
    __str__, __unicode__ = autotext("DynamicRet(task_id={self.task_id} obj_spec={self.obj_spec} ret_prefix={self.ret_prefix})")

    def run(self, results, subnode_results, queue):
        return {".".join(map(str, self.ret_prefix)): results[self.obj_spec]}, None

        
class Ret(AbsTask):
    def __init__(self, obj, ret_prefix, task_id=None):
        super().__init__(task_id=task_id)
        self.obj = obj
        self.ret_prefix = ret_prefix

    __repr__ = autorepr(["task_id", "obj", "ret_prefix"])
    __str__, __unicode__ = autotext("Ret({self.task_id} {self.obj} {self.ret_prefix})")

    def run(self, results, subnode_results, queue):
        return {".".join(map(str, self.ret_prefix)): Right(self.obj)}, None

        
class EndOfQueue(AbsTask):
    def __init__(self):
        super().__init__()

    def __eq__(self, other):
        return isinstance(other, EndOfQueue)

        
def write_to_disk(dqueue, path):
    with open(path, "w") as db:
        while True:
            output = dqueue.get_next_output()
            if output == Nothing:
                break
            else:
                db.write(jsonpickle.encode(output.value) + "\n")


def read_from_disk(path):
    obj = {}
    with open(path) as db:
        for line in db:
            obj.update(jsonpickle.decode(line))
    return obj

        
def mbind(job_run, params, subnode_results, queue):
    resultv = {}
    subnode_resultv = {}
    for k, v in params.items():
        if isinstance(v, Left):
            resultj = v
            ret = {}
            break
        else:
            resultv[k] = v.value
    else:
        for k, v in subnode_results.items():
            if isinstance(v, Left):
                resultj = v
                ret = {}
                break
            else:
                subnode_resultv[k] = v.value
        else:
            # logger.debug(f"mbind: running {job_run}")
            ret, resultj = job_run(resultv, subnode_resultv, queue)
            if not isinstance(resultj, Either):
                resultj = Right(resultj)
    return ret, resultj

                        
def work_on(queue, library_paths):
    logger.debug("library_paths = %s", library_paths)
    sys.path.extend(library_paths)
    while True:
        jri = queue.get()
        job, results, subnode_results, jid = jri
        if isinstance(job, EndOfQueue):
            break
        else:
            # logger.debug(format_message("work_on", "task begin.", {
            #     "job": job,
            #     "jid": jid,
            #     "params": results
            # }))
            logger.debug("task begin %s %s", type(job), jid)
            try:
                logger.info("task begin %s", jid)
                ret, resultj = job.run(results, subnode_results, queue)
            except Exception as e:
                resultj = Left((str(e), traceback.format_exc()))
                ret = {}
            # logger.debug(format_message("work_on", "task complete.", {
            #     "job": job,
            #     "jid": jid,
            #     "ret": ret,
            #     "resultj": resultj,
            #     "params": results
            # }))
            logger.debug("task finish %s %s", type(job), jid)
            logger.info(f"task finish %s", jid)
            queue.complete(jid, ret, Just(resultj))
    

def depends_on(top, v):
    return Just(v["name"]) if "name" in v and v["name"] in top else Nothing


def maybe_to_list(x):
    return x.rec(lambda y: [y], [])

    
def get_python_task_depends_on(top, sub):
    if sub["type"] == "python":
        return {k: name for k, v in sub.get("params", {}).items() for name in maybe_to_list(depends_on(top, v))}
    else:
        raise RuntimeError(f"get_task_depends_on: unsupported task {sub}")


def get_dep_set(subs):
    return {v["name"] for v in subs if v["type"] == "python"}

    
def get_task_depends_on(top, sub):
    if sub["type"] == "python":
        return {name for _, v in sub.get("params", {}).items() for name in maybe_to_list(depends_on(top, v))}
    elif sub["type"] == "map":
        dependencies = get_task_depends_on(top, sub["sub"])
        return dependencies | depends_on(top, sub["coll"]).rec(lambda name: {name}, set())
    elif sub["type"] == "cond":
        dependencies = get_task_depends_on(top, sub["then"])
        dependencies |= get_task_depends_on(top, sub["else"])
        return dependencies | depends_on(top, sub["on"]).rec(lambda name: {name}, set())
    elif sub["type"] == "let":
        return get_task_depends_on(top, sub["sub"])
    elif sub["type"] == "top":
        dep_set = get_dep_set(sub["sub"])
        if len(sub["sub"]) == 0:
            return set()
        else:
            return set.union(*map(partial(get_task_depends_on, top), sub["sub"])) - dep_set
    elif sub["type"] == "ret":
        return depends_on(top, sub["obj"]).rec(lambda name: {name}, set())
    else:
        raise RuntimeError(f"get_task_depends_on: unsupported task {sub}")

    
def get_task_non_dependency_params(top, spec):
    return {k: v for k, v in spec.get("params", {}).items() if depends_on(top, v) == Nothing}


no_op = {
    "type": "top",
    "sub": []
}


# remove tasks that do not provide a return value
def remove_unreachable_tasks(spec):

    def _remove_unreachable_tasks(dg, ret_ids, spec):
    
        # logger.debug(f"remote_unreachable_tasks: spec[\"node_id\"] = {spec['node_id']}")
        if all(spec["node_id"] != a and not dg.is_connected(spec["node_id"], a) for a in ret_ids):
            # logger.debug(f"remote_unreachable_tasks: {spec['node_id']} is unreachable, replace by noop")
            return no_op
        else:
            if spec["type"] == "python":
                return spec
            elif spec["type"] == "map":
                spec["sub"] = _remove_unreachable_tasks(dg, ret_ids, spec["sub"])
                if spec["sub"] == no_op:
                    return no_op
                else:
                    return spec
            elif spec["type"] == "cond":
                spec["then"] = _remove_unreachable_tasks(dg, ret_ids, spec["then"])
                spec["else"] = _remove_unreachable_tasks(dg, ret_ids, spec["else"])
                if spec["then"] == no_op and spec["else"] == no_op:
                    return no_op
                else:
                    return spec
            elif spec["type"] == "let":
                spec["sub"] = _remove_unreachable_tasks(dg, ret_ids, spec["sub"])
                if spec["sub"] == no_op:
                    return no_op
                else:                
                    return spec
            elif spec["type"] == "top":
                spec["sub"] = list(filter(lambda c: c != no_op, map(partial(_remove_unreachable_tasks, dg, ret_ids), spec["sub"])))
                return spec
            elif spec["type"] == "ret":
                return spec
            else:
                raise RuntimeError(f"remove_unreachable_tasks: unsupported task {spec}")

    spec_original = deepcopy(spec)
    dg, ret_ids = dependency_graph(spec)
    # logger.debug(f"remote_unreachable_tasks: dg.edges() = {dg.edges()} ret_ids = {ret_ids}")
    spec_simplified = _remove_unreachable_tasks(dg, ret_ids, spec)
    # logger.debug(f"remove_unreachable_tasks: \n***\n{spec}\n -> \n{spec_simplified}\n&&&")
    return spec_simplified


def dependency_graph(spec):
    g = Graph()
    ret_ids = []
    generate_dependency_graph(g, {}, EnvStack2(), ret_ids, spec, None)
    return g, ret_ids


def generate_dependency_graph(graph, node_map, dep_set, return_ids, sub, parent_node_id):
    node_id = len(graph.nodes())
    graph.add_node(node_id, [sub])
    sub["node_id"] = node_id
    if parent_node_id is not None:
        graph.add_edge(parent_node_id, node_id)

    if sub["type"] == "python":
        for p in sub["params"].values():
            depends_on(dep_set, p).rec(lambda name: graph.add_edge(node_map[name], node_id), None)
        node_map[sub["name"]] = node_id
    elif sub["type"] == "map":
        depends_on(dep_set, sub["coll"]).rec(lambda name: graph.add_edge(node_map[name], node_id), None)
        generate_dependency_graph(graph, node_map, dep_set, return_ids, sub["sub"], node_id)
    elif sub["type"] == "cond":
        depends_on(dep_set, sub["on"]).rec(lambda name: graph.add_edge(node_map[name], node_id), None)
        generate_dependency_graph(graph, node_map, dep_set, return_ids, sub["then"], node_id)
        generate_dependency_graph(graph, node_map, dep_set, return_ids, sub["else"], node_id)
    elif sub["type"] == "let":
        generate_dependency_graph(graph, node_map, dep_set, return_ids, sub["sub"], node_id)
    elif sub["type"] == "top":
        subs = sub["sub"]
        dep_set_sub = EnvStack2(dep_set, get_dep_set(subs))
        for task in sort_tasks(dep_set, subs):
            generate_dependency_graph(graph, node_map, dep_set_sub, return_ids, task, node_id)
    elif sub["type"] == "ret":
        depends_on(dep_set, sub["obj"]).rec(lambda name: graph.add_edge(node_map[name], node_id), None)
        return_ids.append(node_id)
    else:
        raise RuntimeError(f"generate_dependency_graph: unsupported task {sub}")
    

def sort_tasks(dep_set, subs):
    # logger.debug(f"sort_tasks: before: {subs}, dep_set = {dep_set}")
    copy = list(subs)
    subs_sorted = []
    visited = set(dep_set)
    dep_set2 = set(dep_set) | {name for sub in subs for name in maybe_to_list(maybe.from_python(sub.get("name")))}
    while len(copy) > 0:
        copy2 = []
        updated = False
        for sub in copy:
            name = sub.get("name")
            depends_on = get_task_depends_on(dep_set2, sub)
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
            dep = f"visited = {visited}\n"
            for task in copy:
                dep += f"task = {task}\n"
                dep += f"depends_on = {get_task_depends_on(dep_set2, task)}\n"
            raise RuntimeError(f"unresolved dependencies or cycle in depedencies graph {dep}")

    # logger.debug(f"sort_tasks: after: {subs_sorted}")
    return subs_sorted


def split_args(args0):
    kwargs = {k: v for k, v in args0.items() if type(k) == str}
    args = {k: v for k, v in args0.items() if type(k) == int}
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

def generate_tasks(spec, data, top=EnvStack(), ret_prefix=[], hold=set()):
    ty = spec.get("type")
    if ty == "let":
        var = spec["var"]
        obj = spec["obj"]
        sub = spec["sub"]
        data2 = {**data, var: arg_spec_to_arg(data, obj)}
        yield from generate_tasks(sub, data2, top=EnvStack(top), ret_prefix=ret_prefix, hold=hold)
    elif ty == "map":
        coll_name = spec["coll"]
        var = spec["var"]
        subspec = spec["sub"]
        if "name" in coll_name and coll_name["name"] in top:
            # dynamic task
            coll_spec = top[coll_name["name"]]
            subnode_dep = get_task_depends_on(top, subspec)
            subnode_top = {name: top[name] for name in subnode_dep}
            task = DynamicMap(var, coll_spec, subspec, data, subnode_top, ret_prefix)
            dep = {coll_spec}
#            logger.debug(f"add task: {task.task_id} depends_on {dep} : {subnode_dep}")
            yield task, dep | hold, set(subnode_top.values())
        else:
            coll = arg_spec_to_arg(data, coll_name)
            yield from roundrobin(*(generate_tasks(subspec, data2, top=EnvStack(top), ret_prefix=ret_prefix + [i], hold=hold) for i, row in enumerate(coll) if (data2 := {**data, var:row})))
    elif ty == "cond":
        cond_name = spec["on"]
        then_spec = spec["then"]
        else_spec = spec["else"]
        if "name" in cond_name and cond_name["name"] in top:
            cond_spec = top[cond_name["name"]]
            subnode_dep = get_task_depends_on(top, spec)
            subnode_top = {name: top[name] for name in subnode_dep}
            task = DynamicGuard(cond_spec, then_spec, else_spec, data, subnode_top, ret_prefix)
            dep = {cond_spec}
#            logger.debug(f"add task: {task.task_id} depends_on {dep} : {subnode_dep}")
            yield task, dep | hold, set(subnode_top.values())
        else:
            coll = arg_spec_to_arg(data, cond_name)
            if coll:
                yield from generate_tasks(then_spec, data, top=EnvStack(top), ret_prefix=ret_prefix, hold=hold)
            else:
                yield from generate_tasks(else_spec, data, top=EnvStack(top), ret_prefix=ret_prefix, hold=hold)
    elif ty == "top":
        subs = spec["sub"]
        subs_sorted = sort_tasks(top.keys(), subs)
        top = EnvStack(top)
        for sub in subs_sorted:
            yield from generate_tasks(sub, data, top=top, ret_prefix=ret_prefix, hold=hold)
    elif ty == "python":
#        logger.debug(f"add task: dependencies = {get_python_task_depends_on(spec)}\ntop = {top}")
        name = spec["name"]
        mod = spec["mod"]
        func = spec["func"]
        if "task_id" in data:
            raise RuntimeError("task_id cannot be used as a field name")

        args0 = {k: arg_spec_to_arg(data, v) for k, v in get_task_non_dependency_params(top, spec).items()}
        args, kwargs = split_args(args0)
        dependencies = {k: top[v] for k, v in get_python_task_depends_on(top, spec).items()}
        args_spec, kwargs_spec = split_args(dependencies)
        task = Task(name, mod, func, args_spec, kwargs_spec, args, kwargs)
        top[name] = task.task_id
#        logger.debug(f"add task: add task to top. top = {top}")
#        logger.debug(f"add task: {task.task_id} depends_on {dependencies}")
        yield task, set(dependencies.values()) | hold, set()
    elif ty == "ret":
        obj_name = spec["obj"]
        if "name" in obj_name and obj_name["name"] in top:
            obj_spec = top[obj_name["name"]]
            task = DynamicRet(obj_spec, ret_prefix)
            dep = {obj_spec}
            yield task, dep | hold, set()
        else:
            obj = arg_spec_to_arg(data, obj_name)
            yield Ret(obj, ret_prefix), hold, set()
    else:
        raise RuntimeError(f'unsupported spec type {ty}')


def enqueue(spec, data, job_queue, top=EnvStack(), ret_prefix=[], execute_unreachable=False, hold=set()):
    # logger.debug(f"enqueue: data = {data}")

    for what in generate_tasks(spec if execute_unreachable else remove_unreachable_tasks(spec), data, top=top, ret_prefix=ret_prefix, hold=hold):
        job, dependencies, subnode_dependencies = what
        job_id = job.task_id
        logger.debug("add task %s %s\ndepends_on = %s\nsubnode_depends_on = %s", type(job), job_id, dependencies, subnode_dependencies)
        job_queue.put(job, job_id=job_id, depends_on=dependencies, subnode_depends_on=subnode_dependencies)


    

        
        
    
