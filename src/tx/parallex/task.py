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
from .spec import AbsSpec, LetSpec, MapSpec, CondSpec, PythonSpec, SeqSpec, RetSpec, TopSpec, AbsValue, NameValue, DataValue, ret_prefix_to_str, free_names, bound_names, sort_tasks, preproc_tasks, maybe_to_set
import jsonpickle
from tx.readable_log import format_message, getLogger
from typing import List, Any, Dict, Tuple, Set, Callable, TypeVar, ClassVar
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
class IdentifiedTask(AbsTask):
    task_id: str
    
@dataclass
class BaseTask(IdentifiedTask):
    log_error: ClassVar[bool] = False
    
    def run(self, results: Dict[str, Either], subnode_results: Dict[str, Either], queue: DependentQueue) -> Tuple[Dict[str, Either], Either]:
        logger.debug(format_message("BaseTask.run", "start", {"results": results}))
        try:
            return mbind(self.baseRun, results, subnode_results, queue, log_error=self.log_error)
        except Exception as e:
            err = (str(e), traceback.format_exc())
            logger.error(str(err))
            queue.put_output({":error:": Right(err)})
            queue.close()
            raise
            

    @abstractmethod
    def baseRun(self, results: Dict[str, Any], subnode_results: Dict[str, Any], queue: DependentQueue) -> Tuple[Dict[str, Either], Either]:
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

    def baseRun(self, results: Dict[str, Any], subnode_results: Dict[str, Any], queue: DependentQueue) -> Tuple[Dict[str, Either], Either]:
        try:
            logger.debug(format_message("Task.baseRun", "start", {"results": results}))
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
            err = (str(e), traceback.format_exc())
            logger.error(str(err))
            result = Left(err)
        return {}, Right({self.name: result})


@dataclass
class Hold(AbsTask):
    pass


def either_data(results: Dict[str, Any]) -> Dict[str, Either]:
    return {k: Right(v) for k, v in results.items()}


@dataclass
class DynamicMap(BaseTask):
    var: str
    coll_name: str
    spec: AbsSpec
    data: Dict[str, Any]
    ret_prefix: List[Any]
    level: int
    log_error: ClassVar[bool] = True

    def baseRun(self, results: Dict[str, Any], subnode_results: Dict[str, Any], queue: DependentQueue) -> Tuple[Dict[str, Either], Either]:
        hold_id = queue.put(Hold(), is_hold=True)
        logger.debug("DynamicMap.baseRun: put hold task on queue %s", hold_id)
        logger.debug(format_message("DynamicMap.baseRun", "enqueue call", {"results": results, "results[self.coll_name]": results[self.coll_name]}))
        enqueue(
            MapSpec(node_id=None, var=self.var, coll=DataValue(data=results[self.coll_name]), sub=self.spec),
            {**self.data, **either_data(subnode_results)},
            queue, 
            env={},
            ret_prefix=self.ret_prefix,
            execute_original=True,
            hold={hold_id},
            level=self.level
        )
        queue.complete(hold_id, {}, Right({}))
        logger.debug("DynamicMap.baseRun: remove hold task from queue %s", hold_id)
        return {}, Right({})


@dataclass
class DynamicGuard(BaseTask):
    cond_name: str
    then_spec: AbsSpec
    else_spec: AbsSpec
    data: Dict[str, Any]
    ret_prefix: List[Any]
    level: int
    log_error: ClassVar[bool] = True
    
    def baseRun(self, results: Dict[str, Any], subnode_results: Dict[str, Any], queue: DependentQueue) -> Tuple[Dict[str, Either], Either]:
        hold_id = queue.put(Hold(), is_hold=True)
        logger.debug("DynamicCond.baseRun: put hold task on queue %s", hold_id)
        enqueue(
            CondSpec(node_id=None, on=DataValue(data=results[self.cond_name]), then=self.then_spec, _else=self.else_spec),
            {**self.data, **either_data(subnode_results)},
            queue,
            env={},
            ret_prefix=self.ret_prefix,
            execute_original=True,
            hold={hold_id},
            level=self.level
        )
        queue.complete(hold_id, {}, Right({}))
        logger.debug("DynamicCond.baseRun: remove hold task from queue %s", hold_id)
        return {}, Right({})
    

@dataclass
class DynamicLet(BaseTask):
    name: str
    obj_name: str
    
    def baseRun(self, results: Dict[str, Any], subnode_results: Dict[str, Any], queue: DependentQueue) -> Tuple[Dict[str, Either], Either]:
        return {}, Right({self.name: results[self.obj_name]})


@dataclass
class Let(BaseTask):
    name: str
    obj: Either

    def baseRun(self, results: Dict[str, Any], subnode_results: Dict[str, Any], queue: DependentQueue) -> Tuple[Dict[str, Either], Either]:
        return {}, Right({self.name: self.obj})

    
@dataclass
class DynamicRet(IdentifiedTask):
    obj_name: str
    ret_prefix: List[Any]
    
    def run(self, results: Dict[str, Either], subnode_results: Dict[str, Either], queue: DependentQueue) -> Tuple[Dict[str, Either], Either]:
        return {ret_prefix_to_str(self.ret_prefix): results[self.obj_name]}, Right({})


@dataclass
class Ret(IdentifiedTask):
    obj: Either
    ret_prefix: List[Any]

    def run(self, results: Dict[str, Either], subnode_results: Dict[str, Either], queue: DependentQueue) -> Tuple[Dict[str, Either], Either]:
        return {ret_prefix_to_str(self.ret_prefix): self.obj}, Right({})

    
@dataclass
class Seq(BaseTask):
    spec: AbsSpec
    data: Dict[str, Any]
    ret_prefix: List[Any]
    task_id: str
    log_error: ClassVar[bool] = True

    def baseRun(self, results: Dict[str, Any], subnode_results: Dict[str, Any], queue: DependentQueue) -> Tuple[Dict[str, Either], Dict[str, Either]]:
        data = {**self.data, **{name: Right(value) for name, value in results.items()}}
        return evaluate(self.spec, data, self.ret_prefix)

    
@dataclass
class EndOfQueue(AbsTask):
    pass

        
def get_submap(env: Dict[str, Any], fns: Set[str]) -> Dict[str, Any]:
    return {name: env[name] for name in fns if name in env}

    
def evaluate_value(data: Dict[str, Either], arg: AbsValue) -> Either:
    if isinstance(arg, NameValue):
        argnamereference = arg.name
        if not argnamereference in data:
            return Left(f"undefined data {argnamereference}")
        return data[argnamereference]
    elif isinstance(arg, DataValue):
        return Right(arg.data)
    else:
        return Left(f"unsupported value {arg}")


def evaluate(spec: AbsSpec, data: Dict[str, Either], ret_prefix: List[Any]) -> Tuple[Dict[str, Either], Either]:
    logger.debug(format_message("evaluate", "executing sequentially", {"spec": spec, "data": data, "ret_prefix": ret_prefix}))
    if isinstance(spec, LetSpec):
        name = spec.name
        obj = evaluate_value(data, spec.obj)
        return {}, obj.bind(lambda _: {name: obj})
    elif isinstance(spec, MapSpec):
        coll_value = spec.coll
        var = spec.var
        subspec = spec.sub
        coll = evaluate_value(data, coll_value)
        if isinstance(coll, Left):
            return {}, coll
        coll = coll.value
        ret = {}
        for i, row in enumerate(coll):
            data2 = {**data, var:Right(row)}
            sub_ret, sub_result = evaluate(subspec, data2, ret_prefix=ret_prefix + [i])
            ret.update(sub_ret)
            if isinstance(sub_result, Left):
                return ret, sub_result
        return ret, Right({}) # ignore all sub_results
    elif isinstance(spec, CondSpec):
        cond_value = spec.on
        then_spec = spec.then
        else_spec = spec._else
        cond = evaluate_value(data, cond_value)
        if isinstance(cond, Left):
            return {}, cond
        cond = cond.value
        if cond:
            return evaluate(then_spec, data, ret_prefix=ret_prefix)
        else:
            return evaluate(else_spec, data, ret_prefix=ret_prefix)
    elif isinstance(spec, TopSpec):
        subs = spec.sub
        subs_sorted = sort_tasks(set(data.keys()), subs)
        ret = {}
        for sub in subs_sorted:
            sub_ret, sub_result = evaluate(sub, data, ret_prefix=ret_prefix)
            ret.update(sub_ret)
            if isinstance(sub_result, Left):
                return ret, sub_result
            data = {**data, **sub_result.value}
        return ret, Right({})
    elif isinstance(spec, SeqSpec):
        subs = spec.sub
        subs_sorted = sort_tasks(set(data.keys()), subs)
        ret = {}
        result : Dict[str, Either] = {}
        for sub in subs_sorted:
            sub_ret, sub_result = evaluate(sub, data, ret_prefix=ret_prefix)
            ret.update(sub_ret)
            if isinstance(sub_result, Left):
                return ret, sub_result
            data = {**data, **sub_result.value}
            result.update(sub_result.value)
        return ret, Right(result)
    elif isinstance(spec, PythonSpec):
        try:
            mod = import_module(spec.mod) if spec.mod != "" else builtins
            func = getattr(mod, spec.func)
            name = spec.name
            logger.debug(format_message("evaluate", "PythonSpec", {"data": data}))
            args0 = {k: evaluate_value(data, v) for k, v in spec.params.items()}
            errors = [x for x in args0.values() if isinstance(x, Left)]
            if len(errors) > 0:
                return {}, Right({name: errors[0]})
            args2 = {k: v.value for k, v in args0.items()}
            args, kwargs = split_args(args2)
            logger.debug(format_message("evaluate", "PythonSpec before", {"spec.name": spec.name, "spec.mod": spec.mod, "func": lambda: func, "spec.func" : spec.func, "spec.params": spec.params, "args": args, "kwargs": kwargs}))
            
            result = func(*map(lambda x: x[1], sorted(args.items(), key=lambda x: x[0])), **kwargs)
            logger.debug(format_message("evaluate", "PythonSpec after", {"spec.name": spec.name, "spec.mod": spec.mod, "func": lambda: func, "spec.func" : spec.func, "spec.params": spec.params, "args": args, "kwargs": kwargs, "result": result}))
            if not isinstance(result, Either):
                result = Right(result)
        except Exception as e:
            result = Left((str(e), traceback.format_exc()))
        logger.debug(format_message("evaluate", "PythonSpec", {"result": result}))
        return {}, Right({spec.name: result})
    elif isinstance(spec, RetSpec):
        obj_value = spec.obj
        obj = evaluate_value(data, obj_value)
        return {ret_prefix_to_str(ret_prefix): obj}, Right({})
    else:
        raise RuntimeError(f'unsupported spec type {spec}')
    
    
def mbind(job_run : Callable[[Dict[str, Any], Dict[str, Any], DependentQueue], Either], params: Dict[str, Either], subnode_results: Dict[str, Either], queue: DependentQueue, log_error: bool) -> Tuple[Any, Dict[str, Any]]:
    resultv = {}
    subnode_resultv = {}
    for k, v in params.items():
        if isinstance(v, Left):
            resultj = v
            ret: Dict[str, Any] = {}
            if log_error:
                queue.put_output({":error:": Right(v.value)})
            break
        else:
            resultv[k] = v.value
    else:
        for k, v in subnode_results.items():
            if isinstance(v, Left):
                resultj = v
                ret = {}
                if log_error:
                    queue.put_output({":error:": Right(v.value)})
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


def gen_task_name(i: int, spec: AbsSpec) -> str:
    names_sub = bound_names(spec)
    return ",".join(names_sub) if len(names_sub) > 0 else str(i)


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

    
def generate_tasks(queue: DependentQueue, spec: AbsSpec, data: Dict[str, Either], env: Dict[str, str], ret_prefix: List[Any], hold: Set[str], level):
    logger.debug(format_message("generate_tasks", "start", {"spec": spec, "env": env}))

    hold_dep : Dict[str, Set[str]] = {name: set() for name in hold}

    if isinstance(spec, MapSpec):
        coll_value = spec.coll
        var = spec.var
        subspec = spec.sub
        subnode_ret_prefix = ret_prefix + ["@map"]
        if isinstance(coll_value, NameValue) and (coll_name := coll_value.name) not in data:
            coll_source = env[coll_name]
            free_names_sub = free_names(subspec) - {var}
            subnode_env = get_submap(env, free_names_sub)
            subnode_data = get_submap(data, free_names_sub)
            task: IdentifiedTask = DynamicMap(ret_prefix_to_str(subnode_ret_prefix, False), var, coll_name, subspec, subnode_data, ret_prefix, level)
            dep = {coll_source: {coll_name}}
            enqueue_task(queue, task, {**dep, **hold_dep}, inverse_dict(subnode_env))
        else:
            coll = evaluate_value(data, coll_value)

            if isinstance(coll, Left):
                raise RuntimeError(coll.value)

            logger.debug(format_message("generate_tasks", "evaluate_value call ret val", {"data": data, "coll_value": coll_value, "coll": coll}))
            
            for i, row in enumerate(coll.value):
                data_sub = {**data, var: Right(row)}
                subnode_ret_prefix_i = subnode_ret_prefix + [i]
                if level > 0:
                    generate_tasks(queue, subspec, data=data_sub, env=env, ret_prefix=subnode_ret_prefix_i, hold=hold, level=level-1)
                else:
                    free_names_sub = free_names(subspec) - {var}
                    subnode_env = get_submap(env, free_names_sub)
                    subnode_data = get_submap(data, free_names_sub)
                    task = Seq(ret_prefix_to_str(subnode_ret_prefix_i, False), subspec, data_sub, subnode_ret_prefix_i)
                    enqueue_task(queue, task, {**inverse_dict(subnode_env), **hold_dep}, {})

    elif isinstance(spec, CondSpec):
        cond_value = spec.on
        then_spec = spec.then
        else_spec = spec._else
        subnode_ret_prefix = ret_prefix + ["@cond"]
        if isinstance(cond_value, NameValue) and (cond_name := cond_value.name) not in data:
            cond_source = env[cond_name]
            free_names_sub = free_names(spec)
            subnode_env = get_submap(env, free_names_sub)
            subnode_data = get_submap(data, free_names_sub)
            task = DynamicGuard(ret_prefix_to_str(subnode_ret_prefix, False), cond_name, then_spec, else_spec, subnode_data, ret_prefix, level)
            dep = {cond_source: {cond_name}}
            enqueue_task(queue, task, {**dep, **hold_dep}, inverse_dict(subnode_env))
        else:
            cond = evaluate_value(data, cond_value)

            if isinstance(cond, Left):
                raise RuntimeError(cond.value)
            
            if cond.value:
                generate_tasks(queue, then_spec, data=data, env=env, ret_prefix=subnode_ret_prefix + ["@then"], hold=hold, level=level)
            else:
                generate_tasks(queue, else_spec, data=data, env=env, ret_prefix=subnode_ret_prefix + ["@else"], hold=hold, level=level)
    elif isinstance(spec, TopSpec):
        subs = spec.sub
        bound_names_sub_list = [bound_names(sub) for sub in subs]
        bound_names_subs = list(chain(*bound_names_sub_list))
        if len(bound_names_subs) > len(set(bound_names_subs)):
            raise RuntimeError("cannot reuse name in tasks")
        subnode_ret_prefix = ret_prefix + ["@top"]
        task_name_list = [gen_task_name(i, sub) for i, sub in enumerate(subs)]
        env_sub = {**env, **{name: ret_prefix_to_str(subnode_ret_prefix + [task_name], False) for task_name, bound_names_sub in zip(task_name_list, bound_names_sub_list) for name in bound_names_sub}}
        logger.debug(format_message("generate_tasks", "top", {"env_sub": env_sub, "subs": subs}))
        for task_name, sub in zip(task_name_list, subs):
            generate_tasks(queue, sub, data=data, env=env_sub, ret_prefix=subnode_ret_prefix + [task_name], hold=hold, level=level)
    elif isinstance(spec, SeqSpec):
        free_names_sub = free_names(spec)
        env_sub = get_submap(env, free_names_sub)
        data_sub = get_submap(data, free_names_sub)
        ret_prefix_sub = ret_prefix + ["@seq"]
        task = Seq(ret_prefix_to_str(ret_prefix, False), spec, data_sub, ret_prefix=ret_prefix_sub)
        logger.info(format_message("generate_tasks", "generating Seq task", {
            "id": ret_prefix_to_str(ret_prefix, False),
            "data": list(data_sub.keys())
        }))
        enqueue_task(queue, task, {**inverse_dict(env_sub), **hold_dep}, {})
    elif isinstance(spec, LetSpec):
        name = spec.name
        obj_value = spec.obj
        if isinstance(obj_value, NameValue) and (obj_name := obj_value.name) not in data:
            obj_source = env[obj_name]
            task = DynamicLet(env[name], name, obj_name)
            dep = {obj_source: {obj_name}}
            enqueue_task(queue, task, {**dep, **hold_dep}, {})
        else:
            obj = evaluate_value(data, obj_value)
            task = Let(env[name], name, obj)
            enqueue_task(queue, task, {}, {})
    elif isinstance(spec, PythonSpec):
        name = spec.name
        mod = spec.mod
        func = spec.func

        args0 = {k: evaluate_value(data, v) for k, v in spec.params.items() if isinstance(v, DataValue) or (isinstance(v, NameValue) and v.name in data)}
        errors = [x for x in args0.values() if isinstance(x, Left)]        
        if len(errors) > 0:
            raise RuntimeError(errors[0].value)
        args2 = {k: v.value for k, v in args0.items()}
        args, kwargs = split_args(args2)

        dependencies = {k: v.name for k, v in spec.params.items() if isinstance(v, NameValue) and v.name not in data}        
        args_spec, kwargs_spec = split_args(dependencies)
        logger.debug(format_message("generate_tasks", "PythonSpec", {"name": name, "env": env}))
        free_names_sub = free_names(spec)
        env_sub = get_submap(env, free_names_sub)
        task = Task(env[name], name, mod, func, args_spec, kwargs_spec, args, kwargs)
        enqueue_task(queue, task, {**inverse_dict(env_sub), **hold_dep}, {})
        logger.debug(format_message("generate_tasks", "enqueue task", {"task": lambda: vars(task), "env[name]": lambda: env[name]})) 
    elif isinstance(spec, RetSpec):
        obj_value = spec.obj
        subnode_ret_prefix = ret_prefix + ["@ret"]
        if isinstance(obj_value, NameValue) and (obj_name := obj_value.name) not in data:
            obj_source = env[obj_name]
            task = DynamicRet(ret_prefix_to_str(subnode_ret_prefix, False), obj_name, subnode_ret_prefix)
            dep = {obj_source:{obj_name}}
            enqueue_task(queue, task, {**dep, **hold_dep}, {})
        else:
            obj = evaluate_value(data, obj_value)
            task = Ret(ret_prefix_to_str(subnode_ret_prefix, False), obj, subnode_ret_prefix)
            enqueue_task(queue, task, hold_dep, {})
    else:
        raise RuntimeError(f'unsupported spec type {spec}')


def enqueue_task(job_queue: DependentQueue, job: IdentifiedTask, depends_on: Dict[str, Set[str]], subnode_depends_on: Dict[str, Set[str]]):
    
    logger.debug(format_message("enqueue_task", "start", {"input": job, "depends_on": depends_on, "subnode_depends_on": subnode_depends_on}))
    job_id = job.task_id
    logger.debug(format_message("enqueue_task", job_id, {"depends_on": depends_on, "subnode_depends_on": subnode_depends_on}))
    job_queue.put(job, job_id=job_id, depends_on=depends_on, subnode_depends_on=subnode_depends_on)

    
def enqueue(spec: AbsSpec, data: Dict[str, Either], job_queue: DependentQueue, env: Dict[str, str]={}, ret_prefix: List[Any]=[], execute_original: bool=False, hold: Set[str]=set(), level=0):
    generate_tasks(job_queue, spec if execute_original else preproc_tasks(set(data.keys()), spec), data=data, env=env, ret_prefix=ret_prefix, hold=hold, level=level)

    

    

        
        
    
