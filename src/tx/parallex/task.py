import sys
from queue import Queue
from uuid import uuid1
from random import choice
from enum import Enum
from importlib import import_module
from itertools import chain
from more_itertools import roundrobin
import logging
import traceback
from graph import Graph
from functools import partial
from copy import deepcopy
from ctypes import c_int
import builtins
from joblib import Parallel, delayed, parallel_backend
import os
from tx.functional.either import Left, Right, Either
from tx.functional.maybe import Just, Nothing, maybe
from .dependentqueue import DependentQueue
from .utils import inverse_function
from .python import python_to_spec
from .stack import Stack
from .spec import AbsSpec, LetSpec, MapSpec, CondSpec, PythonSpec, SeqSpec, RetSpec, TopSpec, AbsValue, NameValue, DataValue, ret_prefix_to_str, get_task_depends_on, sort_tasks, names, get_python_task_non_dependency_params, get_task_depends_on, preproc_tasks, get_python_task_dependency_params
import jsonpickle
from tx.readable_log import format_message, getLogger
from typing import List, Any, Dict, Tuple, Set, Callable, TypeVar
from dataclasses import dataclass
from abc import ABC, abstractmethod

logger = getLogger(__name__, logging.INFO)

def substitute_list(results, args):
    return [results[arg] for arg in args if arg in results]


def substitute_dict(results, kwargs):
    return {k: results[v] for k, v in kwargs.items() if v in results}
    

@dataclass
class AbsTask(ABC):
    pass

@dataclass
class TaskId(AbsTask):
    task_id: str
    
@dataclass
class BaseTask(TaskId):
    def run(self, results: Dict[str, Any], subnode_results: Dict[str, Any], queue: DependentQueue) -> Tuple[Dict[str, Any], Dict[str, Either]]:
        logger.debug("BaseTask.run: restuls = %s", results)
        return mbind(self.baseRun, results, subnode_results, queue)

    @abstractmethod
    def baseRun(self, results: Dict[str, Any], subnode_results: Dict[str, Any], queue: DependentQueue) -> Tuple[Dict[str, Any], Dict[str, Either]]:
        pass
    

@dataclass
class Task(BaseTask):
    name: str
    mod: str
    func: str
    args_spec: Dict[int, str]
    kwargs_spec: Dict[str, str]
    args: Dict[int, Any]
    kwargs: Dict[str, Any]

    def baseRun(self, results: Dict[str, Any], subnode_results: Dict[str, Any], queue: DependentQueue) -> Tuple[Dict[str, Any], Dict[str, Either]]:
        try:
            mod = import_module(self.mod) if self.mod != "" else builtins
            func = getattr(mod, self.func)
            args = substitute_dict(results, self.args_spec)
            kwargs = substitute_dict(results, self.kwargs_spec)
            self_arg_items = self.args.items()
            arg_items = args.items()
            result = func(*[v for _, v in sorted(chain(self_arg_items, arg_items), key=lambda x: x[0])], **self.kwargs, **kwargs)
            if not isinstance(result, Either):
                result = Right(result)
        except Exception as e:
            result = Left((str(e), traceback.format_exc()))
        return {}, Right({self.name: result})


@dataclass
class Hold(AbsTask):
    pass


@dataclass
class DynamicMap(BaseTask):
    var: str
    coll_name: str
    spec: AbsSpec
    data: Dict[str, Any]
    subnode_env: Dict[str, str]
    ret_prefix: List[Any]
    level: int

    def baseRun(self, results: Dict[str, Any], subnode_results: Dict[str, Any], queue: DependentQueue) -> Tuple[Dict[str, Any], Dict[str, Either]]:
        hold_id = queue.put(Hold(), is_hold=True)
        logger.debug("DynamicMap.baseRun: put hold task on queue %s", hold_id)
        enqueue(MapSpec(node_id=None, var=self.var, coll=DataValue(data=results[self.coll_name]), sub=self.spec), {**self.data, **{name: subnode_results[node_id] for name, node_id in self.subnode_env.items()}}, queue, env={}, ret_prefix=self.ret_prefix, execute_original=True, hold={hold_id}, level=self.level)
        queue.complete(hold_id, {}, Right({}))
        logger.debug("DynamicMap.baseRun: remove hold task from queue %s", hold_id)
        return {}, Right({})


@dataclass
class DynamicGuard(BaseTask):
    cond_name: str
    then_spec: AbsSpec
    else_spec: AbsSpec
    data: Dict[str, Any]
    subnode_top: Dict[str, str]
    ret_prefix: List[Any]
    
    def baseRun(self, results: Dict[str, Any], subnode_results: Dict[str, Any], queue: DependentQueue) -> Tuple[Dict[str, Any], Dict[str, Either]]:
        logger.debug("DynamicCond.baseRun: before hold")
        hold_id = queue.put(Hold(), is_hold=True)
        logger.debug("DynamicCond.baseRun: put hold task on queue %s", hold_id)
        enqueue(CondSpec(node_id=None, on=DataValue(data=results[self.cond_name]), then=self.then_spec, _else=self.else_spec), {**self.data, **{name: subnode_results[node_id] for name, node_id in self.subnode_top.items()}}, queue, env={}, ret_prefix=self.ret_prefix, execute_original=True, hold={hold_id})
        queue.complete(hold_id, {}, Right({}))
        logger.debug("DynamicCond.baseRun: remove hold task from queue %s", hold_id)
        return {}, Right({})
    

@dataclass
class DynamicRet(TaskId):
    obj_name: str
    ret_prefix: List[Any]
    
    def run(self, results: Dict[str, Any], subnode_results: Dict[str, Any], queue: DependentQueue) -> Tuple[Dict[str, Any], Dict[str, Either]]:
        return {ret_prefix_to_str(self.ret_prefix): results[self.obj_name]}, Right({})


@dataclass
class Ret(TaskId):
    obj: Any
    ret_prefix: List[Any]

    def run(self, results: Dict[str, Any], subnode_results: Dict[str, Any], queue: DependentQueue) -> Tuple[Dict[str, Any], Dict[str, Either]]:
        return {ret_prefix_to_str(self.ret_prefix): self.obj}, Right({})

    
@dataclass
class Seq(BaseTask):
    spec: AbsSpec
    data: Dict[str, Any]
    depends_on: Set[str]
    ret_prefix: List[Any]
    task_id: str

    def baseRun(self, results: Dict[str, Any], subnode_results: Dict[str, Any], queue: DependentQueue) -> Tuple[Dict[str, Any], Dict[str, Either]]:
        data = {**{k:Right(v) for k,v in self.data.items()}, **{name: results[name] for name in self.depends_on}}
        return execute(self.spec, data, self.ret_prefix)

    
@dataclass
class EndOfQueue(AbsTask):
    pass

        
def get_task_depends_on_dict(env: Dict[str, Any], spec):
    dep = get_task_depends_on(set(env.keys()), spec)
    env = {name: env[name] for name in dep}
    return env

    
def execute(spec: AbsSpec, data: Dict[str, Either], ret_prefix: List[Any]) -> Tuple[Dict[str, Any], Either]:
    logger.debug(format_message("execute", "executing sequentially", {"spec": spec, "data": data, "ret_prefix": ret_prefix}))
    if isinstance(spec, LetSpec):
        var = spec.var
        obj = spec.obj
        sub = spec.sub
        data2 = {**data, var: arg_spec_to_arg_error(data, obj)}
        return execute(sub, data2, ret_prefix=ret_prefix)
    elif isinstance(spec, MapSpec):
        coll_value = spec.coll
        var = spec.var
        subspec = spec.sub
        coll = arg_spec_to_arg_error(data, coll_value)
        if isinstance(coll, Left):
            return {}, coll
        coll = coll.value
        ret = {}
        for i, row in enumerate(coll):
            data2 = {**data, var:Right(row)}
            sub_ret, sub_result = execute(subspec, data2, ret_prefix=ret_prefix + [i])
            if isinstance(sub_result, Left):
                return {}, sub_result
            ret.update(sub_ret)
        return ret, Right({}) # ignore all sub_results
    elif isinstance(spec, CondSpec):
        cond_value = spec.on
        then_spec = spec.then
        else_spec = spec._else
        cond = arg_spec_to_arg_error(data, cond_value)
        if isinstance(cond, Left):
            return {}, cond
        cond = cond.value
        if cond:
            return execute(then_spec, data, ret_prefix=ret_prefix)
        else:
            return execute(else_spec, data, ret_prefix=ret_prefix)
    elif isinstance(spec, TopSpec):
        subs = spec.sub
        subs_sorted = sort_tasks(set(data.keys()), subs)
        ret = {}
        for sub in subs_sorted:
            sub_ret, sub_result = execute(sub, data, ret_prefix=ret_prefix)
            if isinstance(sub_result, Left):
                return {}, sub_result
            ret.update(sub_ret)
            data = {**data, **sub_result.value}
        return ret, Right({})
    elif isinstance(spec, SeqSpec):
        subs = spec.sub
        subs_sorted = sort_tasks(set(data.keys()), subs)
        ret = {}
        for sub in subs_sorted:
            sub_ret, sub_result = execute(sub, data, ret_prefix=ret_prefix)
            logger.debug(format_message("execute", "SeqSpec", {"sub_result": sub_result}))
            if isinstance(sub_result, Left):
                return {}, sub_result
            ret.update(sub_ret)
            data = {**data, **sub_result.value}
        return ret, {name: data[name] for name in spec.names}
    elif isinstance(spec, PythonSpec):
        try:
            mod = import_module(spec.mod) if spec.mod != "" else builtins
            func = getattr(mod, spec.func)
            logger.debug(format_message("execute", "PythonSpec", {"data": data}))
            args0 = {k: arg_spec_to_arg_error(data, v) for k, v in spec.params.items()}
            errors = [x for x in args0.values() if isinstance(x, Left)]
            if len(errors) > 0:
                return {}, errors[0]
            args2 = {k: v.value for k, v in args0.items()}
            args, kwargs = split_args(args2)
            logger.debug(format_message("execute", "PythonSpec", {}))
            
            result = func(*map(lambda x: x[1], sorted(args.items(), key=lambda x: x[0])), **kwargs)
            logger.debug(format_message("execute", "PythonSpec", {"spec.name": spec.name, "spec.mod": spec.mod, "func": lambda: func, "spec.func" : spec.func, "spec.params": spec.params, "args": args, "kwargs": kwargs, "result": result, "func(1)": func(1)}))
            if not isinstance(result, Either):
                result = Right(result)
        except Exception as e:
            result = Left((str(e), traceback.format_exc()))
        logger.debug(format_message("execute", "PythonSpec", {"result": result}))
        return {}, Right({spec.name: result})
    elif isinstance(spec, RetSpec):
        obj_value = spec.obj
        obj = arg_spec_to_arg_error(data, obj_value)
        return {ret_prefix_to_str(ret_prefix): obj}, Right({})
    else:
        raise RuntimeError(f'unsupported spec type {spec}')
    
    
def mbind(job_run : Callable[[Dict[str, Any], Dict[str, Any], DependentQueue], Either], params: Dict[str, Either], subnode_results: Dict[str, Either], queue: DependentQueue) -> Tuple[Any, Dict[str, Any]]:
    resultv = {}
    subnode_resultv = {}
    for k, v in params.items():
        if isinstance(v, Left):
            resultj = v
            ret: Dict[str, Any] = {}
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
    return ret, resultj

                        
def split_args(args0):
    kwargs = {k: v for k, v in args0.items() if type(k) == str}
    args = {k: v for k, v in args0.items() if type(k) == int}
    return args, kwargs


def arg_spec_to_arg(data: Dict[str, Any], arg: AbsValue):
    if isinstance(arg, NameValue):
        argnamereference = arg.name
        if not argnamereference in data:
            raise RuntimeError(f"undefined data {argnamereference}")
        return data[argnamereference]
    elif isinstance(arg, DataValue):
        return arg.data
    else:
        raise RuntimeError(f"unsupported value {arg}")


def arg_spec_to_arg_error(data: Dict[str, Either], arg: AbsValue):
    if isinstance(arg, NameValue):
        argnamereference = arg.name
        if not argnamereference in data:
            return Left(f"undefined data {argnamereference}")
        return data[argnamereference]
    elif isinstance(arg, DataValue):
        return Right(arg.data)
    else:
        return Left(f"unsupported value {arg}")


def task_name(i: int, spec: AbsSpec, container_type: str) -> str:
    names_sub = names(spec)
    return "\"".join(names_sub) if len(names_sub) > 0 else f"@{container_type}{i}"


S = TypeVar("S")
T = TypeVar("T")

def inverse_dict(env: Dict[S, T]) -> Dict[T, Set[S]]:
    dep : Dict[T, Set[S]] = {}
    for k, v in env.items():
        if v in dep:
            dep[v].add(k)
        else:
            dep[v] = {k}
    return dep

    
def generate_tasks(queue: DependentQueue, spec: AbsSpec, data: Dict[str, Any], env: Dict[str, str], ret_prefix: List[Any], hold: Set[str], level):
    logger.debug(format_message("generate_tasks", "start", {"spec": spec, "env": env}))
    if isinstance(spec, LetSpec):
        var = spec.var
        obj = spec.obj
        sub = spec.sub
        data2 = {**data, var: arg_spec_to_arg(data, obj)}
        generate_tasks(queue, sub, data2, env=env, ret_prefix=ret_prefix + ["@let"], hold=hold, level=level)
    elif isinstance(spec, MapSpec):
        coll_value = spec.coll
        var = spec.var
        subspec = spec.sub
        if isinstance(coll_value, NameValue) and coll_value.name in env:
            # dynamic task
            coll_source = env[coll_value.name]
            subnode_env = get_task_depends_on_dict(env, subspec)
            subnode_ret_prefix = ret_prefix + ["@map"]
            task: TaskId = DynamicMap(ret_prefix_to_str(subnode_ret_prefix, False), var, coll_value.name, subspec, data, subnode_env, subnode_ret_prefix, level)
            dep = {coll_source: {coll_value.name}}
            enqueue_task(queue, task, {**dep, **{name: set() for name in hold}}, inverse_dict(subnode_env))
        else:
            coll = arg_spec_to_arg(data, coll_value)
            def generate_tasks_for_item(i, row):
                data2 = {**data, var:row}
                if level > 0:
                    generate_tasks(queue, subspec, data2, env=env, ret_prefix=ret_prefix + ["@map", i], hold=hold, level=level-1)
                else:
                    subnode_ret_prefix = ret_prefix + ["@map", i]
                    subnode_env = get_task_depends_on_dict(env, subspec)
                    task = Seq(ret_prefix_to_str(subnode_ret_prefix, False), subspec, data2, set(subnode_env.keys()), subnode_ret_prefix)
                    enqueue_task(queue, task, {**inverse_dict(subnode_env), **{name: set() for name in hold}}, {})

            for i, row in enumerate(coll):
                generate_tasks_for_item(i, row)
    elif isinstance(spec, CondSpec):
        cond_value = spec.on
        then_spec = spec.then
        else_spec = spec._else
        if isinstance(cond_value, NameValue) and cond_value.name in env:
            cond_source = env[cond_value.name]
            subnode_env = get_task_depends_on_dict(env, spec)
            subnode_ret_prefix = ret_prefix + ["@cond"]
            task = DynamicGuard(ret_prefix_to_str(subnode_ret_prefix, False), cond_value.name, then_spec, else_spec, data, subnode_env, subnode_ret_prefix)
            dep = {cond_source: {cond_value.name}}
            enqueue_task(queue, task, {**dep, **{name: set() for name in hold}}, inverse_dict(subnode_env))
        else:
            cond = arg_spec_to_arg(data, cond_value)
            if cond:
                generate_tasks(queue, then_spec, data, env=env, ret_prefix=ret_prefix + ["@cond", "@then"], hold=hold, level=level)
            else:
                generate_tasks(queue, else_spec, data, env=env, ret_prefix=ret_prefix + ["@cond", "@else"], hold=hold, level=level)
    elif isinstance(spec, TopSpec):
        subs = spec.sub
        subs_names = [name for sub in subs for name in names(sub)]
        if len(subs_names) > len(set(subs_names)):
            raise RuntimeError("cannot reuse name in tasks")
        sub_env = dict(env)
        for i, sub in enumerate(subs):
            sub_names : Set[str] = names(sub)
            for name in sub_names:
                sub_env[name] = ret_prefix_to_str(ret_prefix + [task_name(i, sub, "top")], False)
        for i, sub in enumerate(subs):
            generate_tasks(queue, sub, data, env=sub_env, ret_prefix=ret_prefix + [task_name(i, sub, "top")], hold=hold, level=level)
    elif isinstance(spec, SeqSpec):
        dependencies_dict = get_task_depends_on_dict(env, spec) 
        task = Seq(spec=spec, data=data, depends_on=set(dependencies_dict.keys()), task_id=ret_prefix_to_str(ret_prefix, False), ret_prefix=ret_prefix)
        enqueue_task(queue, task, {**inverse_dict(dependencies_dict), **{name: set() for name in hold}}, {})
    elif isinstance(spec, PythonSpec):
        name = spec.name
        mod = spec.mod
        func = spec.func
        if "task_id" in data:
            raise RuntimeError("task_id cannot be used as a field name")

        args0 = {k: arg_spec_to_arg(data, v) for k, v in get_python_task_non_dependency_params(set(env.keys()), spec).items()}
        args, kwargs = split_args(args0)
        dependencies = get_python_task_dependency_params(set(env.keys()), spec)
        depends_on = get_task_depends_on_dict(env, spec)
        args_spec, kwargs_spec = split_args(dependencies)
        logger.debug(format_message("generate_tasks", "PythonSpec", {"name": name, "env": env}))
        task = Task(env[name], name, mod, func, args_spec, kwargs_spec, args, kwargs)
        
#        logger.debug(f"add task: add task to top. top = {top}")
#        logger.debug(f"add task: {task.task_id} depends_on {dependencies}")
        enqueue_task(queue, task, {**inverse_dict(depends_on), **{name: set() for name in hold}}, {})
        logger.debug(format_message("generate_tasks", "enqueue task", {"task": lambda: vars(task), "env[name]": lambda: env[name]})) 
    elif isinstance(spec, RetSpec):
        obj_value = spec.obj
        subnode_ret_prefix = ret_prefix + ["@ret"]
        if isinstance(obj_value, NameValue) and obj_value.name in env:
            obj_source = env[obj_value.name]
            task = DynamicRet(ret_prefix_to_str(subnode_ret_prefix, False), obj_value.name, subnode_ret_prefix)
            dep = {obj_source:{obj_value.name}}
            enqueue_task(queue, task, {**dep, **{name: set() for name in hold}}, {})
        else:
            obj = Right(arg_spec_to_arg(data, obj_value))
            task = Ret(ret_prefix_to_str(subnode_ret_prefix, False), obj, subnode_ret_prefix)
            enqueue_task(queue, task, {name: set() for name in hold}, {})
    else:
        raise RuntimeError(f'unsupported spec type {spec}')


def enqueue_task(job_queue: DependentQueue, job: TaskId, depends_on: Dict[str, Set[str]], subnode_depends_on: Dict[str, Set[str]]):
    
    logger.debug(format_message("enqueue_task", "start", {"input": job, "depends_on": depends_on, "subnode_depends_on": subnode_depends_on}))
    job_id = job.task_id
    logger.debug(format_message("enqueue_task", job_id, {"depends_on": depends_on, "subnode_depends_on": subnode_depends_on}))
    job_queue.put(job, job_id=job_id, depends_on=depends_on, subnode_depends_on=subnode_depends_on)

    
def enqueue(spec: AbsSpec, data: Dict[str, Any], job_queue: DependentQueue, env: Dict[str, str]={}, ret_prefix: List[Any]=[], execute_original: bool=False, hold: Set[str]=set(), level=0):
    generate_tasks(job_queue, spec if execute_original else preproc_tasks(spec), data, env=env, ret_prefix=ret_prefix, hold=hold, level=level)

    

    

        
        
    
