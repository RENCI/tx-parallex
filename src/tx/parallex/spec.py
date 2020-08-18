import sys
from queue import Queue
from uuid import uuid4
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
from tx.functional.maybe import Just, Nothing, maybe, Maybe
from .dependentqueue import DependentQueue
from .utils import inverse_function
from .python import python_to_spec
from .stack import Stack
import jsonpickle
from tx.readable_log import format_message, getLogger
from typing import List, Any, Dict, Tuple, Callable, Set, Optional
from dataclasses import dataclass, field
from abc import ABC

logger = getLogger(__name__, logging.INFO)

def maybe_to_list(x: Maybe) -> list:
    return x.rec(lambda y: [y], [])
    

def maybe_to_set(x: Maybe) -> set:
    return x.rec(lambda y: {y}, set())
    

class AbsValue(ABC):
    pass


@dataclass
class NameValue(AbsValue):
    name: str

    
@dataclass        
class DataValue(AbsValue):
    data: Any
        

@dataclass
class AbsSpec(ABC):
    node_id: Optional[str]


@dataclass
class MapSpec(AbsSpec):
    coll: AbsValue
    var: str
    sub: AbsSpec


@dataclass
class LetSpec(AbsSpec):
    name: str
    obj: AbsValue

        
@dataclass
class PythonSpec(AbsSpec):
    name: str
    mod: str
    func: str
    params: Dict[str, AbsValue] = field(default_factory=dict)


@dataclass
class RetSpec(AbsSpec):
    obj: AbsValue


@dataclass
class TopSpec(AbsSpec):
    sub: List[AbsSpec]

    
@dataclass
class CondSpec(AbsSpec):
    on: AbsValue
    then: AbsSpec
    _else: AbsSpec
    

@dataclass
class SeqSpec(AbsSpec):
    sub: List[AbsSpec] # a sequence of tasks with the last task in the sequence return a value


def dict_to_value(x: dict) -> AbsValue:
    if "name" in x:
        return NameValue(x["name"])
    else:
        return DataValue(x["data"])


def dict_to_spec(x: dict) -> AbsSpec:
    logger.debug(format_message("dict_to_spec", "convert", {"x": x}))
    ty = x["type"]
    
    if ty == "let":
        return LetSpec(name=x["name"], obj=dict_to_value(x["obj"]), node_id=None)
    elif ty == "map":
        return MapSpec(coll=dict_to_value(x["coll"]), var=x["var"], sub=dict_to_spec(x["sub"]), node_id=None)
    elif ty == "cond":
        return CondSpec(on=dict_to_value(x["on"]), then=dict_to_spec(x["then"]), _else=dict_to_spec(x["else"]), node_id=None)
    elif ty == "top":
        return TopSpec(sub=[dict_to_spec(sub) for sub in x["sub"]], node_id=None)
    elif ty == "seq":
        return SeqSpec(sub=[dict_to_spec(sub) for sub in x["sub"]], node_id=None)
    elif ty == "ret":
        return RetSpec(obj=dict_to_value(x["obj"]), node_id=None)
    elif ty == "python":
        return PythonSpec(name=x["name"], mod=x["mod"], func=x["func"], params={k: dict_to_value(v) for k,v in x.get("params", {}).items()}, node_id=None)
    else:
        raise RuntimeError(f"unsupported dict {x}")


# return a set of names that a spec provides values for
def bound_names(spec: AbsSpec) -> Set[str]:
    if isinstance(spec, PythonSpec):
        return {spec.name}
    elif isinstance(spec, LetSpec):
        return {spec.name}
    elif isinstance(spec, SeqSpec):
        return bound_names_list(spec.sub)
    else:
        return set()


def bound_names_list(subs: List[AbsSpec]) -> Set[str]:
    if len(subs) == 0:
        return set()
    else:
        return set.union(*map(bound_names, subs))

    
# give a set of names provided by task in the current or outer scope, return a name that an AbsValue depends on if any
def free_names_value(v: AbsValue) -> Maybe:
    return Just(v.name) if isinstance(v, NameValue) else Nothing


def compose(f, g):
    return lambda x: f(g(x))


def free_names(spec: AbsSpec) -> Set[str]:
    if isinstance(spec, PythonSpec):
        params = spec.params
        if len(params) == 0:
            return set()
        else:
            return set.union(*map(compose(maybe_to_set, free_names_value), params.values()))
    elif isinstance(spec, LetSpec):
        return maybe_to_set(free_names_value(spec.obj))
    elif isinstance(spec, MapSpec):
        free_names_sub = free_names(spec.sub)
        return (free_names_sub - {spec.var}) | maybe_to_set(free_names_value(spec.coll))
    elif isinstance(spec, CondSpec):
        free_names_then = free_names(spec.then)
        free_names_else = free_names(spec._else)
        return free_names_then | free_names_else | maybe_to_set(free_names_value(spec.on))
    elif isinstance(spec, TopSpec):
        if len(spec.sub) == 0:
            return set()
        else:
            bound_names_sub = bound_names_list(spec.sub)
            return set.union(*map(free_names, spec.sub)) - bound_names_sub
    elif isinstance(spec, SeqSpec):
        if len(spec.sub) == 0:
            return set()
        else:
            bound_names_sub = bound_names_list(spec.sub)
            return set.union(*map(free_names, spec.sub)) - bound_names_sub
    elif isinstance(spec, RetSpec):
        return maybe_to_set(free_names_value(spec.obj))
    else:
        raise RuntimeError(f"free_names: unsupported task {spec}")

    
no_op = TopSpec(node_id=None, sub=[])


def ret_prefix_to_str(ret_prefix, exclude_str=True):
    return ".".join(map(str, filter(lambda x : not isinstance(x, str), ret_prefix) if exclude_str else ret_prefix))


def sort_tasks(env: Set[str], subs: List[AbsSpec]) -> List[AbsSpec]:
    # logger.debug(f"sort_tasks: before: {subs}, env = {env}")
    copy = list(subs)
    subs_sorted = []
    visited = set(env)
    while len(copy) > 0:
        copy2 = []
        updated = False
        for sub in copy:
            bound_names_sub = bound_names(sub)
            free_names_sub = free_names(sub)
            if len(free_names_sub - visited) == 0:
                visited |= bound_names_sub
                subs_sorted.append(sub)
                updated = True
            else:
                copy2.append(sub)
        if updated:
            copy = copy2
        else:
            logger.error(format_message("sort_tasks:", "unresolved dependencies or cycle in depedencies graph", {
                "visited": visited
            }))
            for task in copy:
                logger.error(format_message("sort_tasks:", "remaining task", {
                    "task": task,
                    "free_names(task)": free_names(task),
                    "free_names(task) - visisted": free_names(task) - visited
                }))
            raise RuntimeError("unresolved dependencies or cycle in depedencies graph")

    # logger.debug(f"sort_tasks: after: {subs_sorted}")
    return subs_sorted


def dependency_graph(inputs : Set[str],  spec: AbsSpec) -> Tuple[Graph, Set[str]]:
    g = Graph()
    ret_ids : Set[str] = set()
    input_node = g.add_node("@input")
    generate_dependency_graph(g, {inp : input_node for inp in inputs}, ret_ids, spec, inputs, [], None)
    return g, ret_ids


def has_ret(spec: AbsSpec):
    if isinstance(spec, PythonSpec):
        return False
    elif isinstance(spec, MapSpec):
        return has_ret(spec.sub)
    elif isinstance(spec, CondSpec):
        return has_ret(spec.then) or has_ret(spec._else)
    elif isinstance(spec, LetSpec):
        return False
    elif isinstance(spec, TopSpec):
        return any(map(has_ret, spec.sub))
    elif isinstance(spec, SeqSpec):
        return any(map(has_ret, spec.sub))
    elif isinstance(spec, RetSpec):
        return True
    else:
        raise RuntimeError(f"has_ret: unsupported task {spec}")


def generate_dependency_graph(graph: Graph, node_map: Dict[str, str], return_ids: Set[str], spec: AbsSpec, env: Set[str], static_ret_prefix: List[str], parent_node_id: Optional[str]):
    logger.debug(format_message("generate_dependency_graph", "start", {"node_map": node_map, "spec": spec, "env": env}))
    node_id = ret_prefix_to_str(static_ret_prefix, False)
    graph.add_node(node_id, spec)
    spec.node_id = node_id
    if parent_node_id is not None:
        graph.add_edge(parent_node_id, node_id)

    for name in bound_names(spec):
        node_map[name] = node_id
        
    if isinstance(spec, PythonSpec):
        for p in spec.params.values():
            free_names_value(p).rec(lambda name: graph.add_edge(node_map[name], node_id), None)
    elif isinstance(spec, MapSpec):
        free_names_value(spec.coll).rec(lambda name: graph.add_edge(node_map[name], node_id), None)
        env2 = env | {spec.var}
        node_map[spec.var] = node_id
        generate_dependency_graph(graph, node_map, return_ids, spec.sub, env2, static_ret_prefix + ["@map"], node_id)
    elif isinstance(spec, CondSpec):
        free_names_value(spec.on).rec(lambda name: graph.add_edge(node_map[name], node_id), None)
        generate_dependency_graph(graph, node_map, return_ids, spec.then, env, static_ret_prefix + ["@then"], node_id)
        generate_dependency_graph(graph, node_map, return_ids, spec._else, env, static_ret_prefix + ["@else"], node_id)
    elif isinstance(spec, LetSpec):
        free_names_value(spec.obj).rec(lambda name: graph.add_edge(node_map[name], node_id), None)
    elif isinstance(spec, TopSpec):
        subs = spec.sub
        env2 = env | bound_names_list(subs)
        for i, task in enumerate(sort_tasks(env, subs)):
            generate_dependency_graph(graph, node_map, return_ids, task, env2, static_ret_prefix + [f"@top{i}"], node_id)
    elif isinstance(spec, SeqSpec):
        fns = free_names(spec)
        for name in fns:
            graph.add_edge(node_map[name], node_id)
        if has_ret(spec):
            return_ids.add(node_id)
    elif isinstance(spec, RetSpec):
        free_names_value(spec.obj).rec(lambda name: graph.add_edge(node_map[name], node_id), None)
        return_ids.add(node_id)
    else:
        raise RuntimeError(f"generate_dependency_graph: unsupported task {spec}")
    

# remove tasks that do not provide a return value
def remove_unreachable_tasks(dg: Graph, ret_ids: Set[str], spec: AbsSpec) -> AbsSpec:
    # logger.debug(f"remote_unreachable_tasks: spec[\"node_id\"] = {spec['node_id']}")
    if all(spec.node_id != a and not dg.is_connected(spec.node_id, a) for a in ret_ids):
        # logger.debug(f"remote_unreachable_tasks: {spec['node_id']} is unreachable, replace by noop")
        return no_op
    else:
        if isinstance(spec, PythonSpec):
            return spec
        elif isinstance(spec, MapSpec):
            sub = remove_unreachable_tasks(dg, ret_ids, spec.sub)
            if sub == no_op:
                return no_op
            else:
                spec.sub = sub
                return spec
        elif isinstance(spec, CondSpec):
            then = remove_unreachable_tasks(dg, ret_ids, spec.then)
            _else = remove_unreachable_tasks(dg, ret_ids, spec._else)
            if then == no_op and _else == no_op:
                return no_op
            else:
                spec.then = then
                spec._else = _else
                return spec
        elif isinstance(spec, LetSpec):
            return spec
        elif isinstance(spec, TopSpec):
            subs = list(filter(lambda c: c != no_op, map(partial(remove_unreachable_tasks, dg, ret_ids), spec.sub)))
            spec.sub = subs
            return spec
        elif isinstance(spec, SeqSpec):
            return spec
        elif isinstance(spec, RetSpec):
            return spec
        else:
            raise RuntimeError(f"remove_unreachable_tasks: unsupported task {spec}")

        
def propagate_constants(dg: Graph, ret_ids: Set[str], spec: AbsSpec) -> AbsSpec:
    return spec

        
def combine_sequential_tasks(dg: Graph, ret_ids: Set[str], spec: AbsSpec) -> AbsSpec:
    return spec


def preproc_tasks(inputs: Set[str], spec: AbsSpec) -> AbsSpec :

    spec_original = deepcopy(spec)
    dg, ret_ids = dependency_graph(inputs, spec)
    # logger.debug(f"remote_unreachable_tasks: dg.edges() = {dg.edges()} ret_ids = {ret_ids}")
    spec_simplified = remove_unreachable_tasks(dg, ret_ids, spec)
    spec_simplified = propagate_constants(dg, ret_ids, spec)
    spec_combined = combine_sequential_tasks(dg, ret_ids, spec_simplified)
    # logger.debug(f"remove_unreachable_tasks: \n***\n{spec}\n -> \n{spec_simplified}\n&&&")
    return spec_combined


    
