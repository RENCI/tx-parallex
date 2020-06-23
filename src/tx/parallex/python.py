from queue import Queue
from uuid import uuid4
from random import choice
from enum import Enum
from importlib import import_module
from more_itertools import roundrobin
from itertools import chain
from autorepr import autorepr, autotext
from multiprocessing import Manager
from ast import parse, Call, Name, UnaryOp, Constant, List, Dict, Return, For, Assign, If
import logging
from tx.functional.either import Left, Right, Either
from .dependentqueue import DependentQueue, SubQueue
from .stack import Stack


logging.basicConfig(level=logging.INFO)


logger = logging.getLogger(__name__)


def python_ast_to_value(expr):
    if isinstance(expr, List):
        return [python_ast_to_value(elt) for elt in expr.elts]
    elif isinstance(expr, Dict):
        return {python_ast_to_value(k): python_ast_to_value(v) for k, v in zip(expr.keys, expr.values)}
    elif isinstance(expr, Constant):
        return expr.value
    else:
        raise RuntimeError(f"cannot convert ast {expr} to value")

    
def python_ast_to_arg(expr):
    if isinstance(expr, Name):
        return {
            "name": expr.id
        }
    else:
        return {
            "data": python_ast_to_value(expr)
        }

    
def let_spec(body, func):
    assigns = [stmt for stmt in body if not isinstance(stmt, Return) and not isinstance(stmt, For) and not isinstance(stmt, If) and not isinstance(stmt.value, Call)]
    rest = [stmt for stmt in body if stmt not in assigns]
    spec = func(rest)
    def let_spec_handle_assign(assigns, spec):
        if len(assigns) == 0:
            return spec
        else:
            *resta, assign = assigns
            spec_wrapped_by_assign = {
                "type": "let",
                "var": assign.targets[0].id,
                "obj": python_ast_to_arg(assign.value),
                "sub": spec
            }
            return let_spec_handle_assign(resta, spec_wrapped_by_assign)
    return let_spec_handle_assign(assigns, spec)
    
def python_to_spec(py):
    t = parse(py)
    body = t.body
    
    return python_to_spec_seq(body, EnvStack2())
    
    
def python_to_spec_seq(body, dep_set):
    def func(stmts):
            apps = [stmt for stmt in stmts if (isinstance(stmt, Assign) and isinstance(stmt.value, Call))]
            fors = [stmt for stmt in stmts if isinstance(stmt, For)]
            ifs = [stmt for stmt in stmts if isinstance(stmt, If)]
            returns = [stmt for stmt in stmts if isinstance(stmt, Return)]
            dep_set2 = dep_set | {app.targets[0].id for app in apps}
    
            return python_to_top_spec(apps + fors + ifs + returns, dep_set2)
    
    return let_spec(body, func)


def python_to_top_spec(body, dep_set):
    specs = list(chain.from_iterable(python_to_spec_in_top(stmt, dep_set) for stmt in body))
    if len(specs) == 1:
        return specs[0]
    else:
        return {
            "type": "top",
            "sub": specs
        }


EnvStack2 = Stack(set())

def python_ast_to_term(dep_set, iter):
    if isinstance(iter, Name):
        if iter.id in dep_set:
            coll_name = {
                "depends_on": iter.id
            }
        else:
            coll_name = {
                "name": iter.id
            }
    else:
        coll_name = {
            "data": python_ast_to_value(iter)
        }
    return coll_name

def python_to_spec_in_top(stmt, dep_set):
    if isinstance(stmt, For):
        coll_name = python_ast_to_term(dep_set, stmt.iter)
        return [{
            "type": "map",
            "var": stmt.target.id,
            "coll": coll_name,
            "sub": python_to_spec_seq(stmt.body, EnvStack2(dep_set))
        }]
    elif isinstance(stmt, If):
        cond_name = python_ast_to_term(dep_set, stmt.test)
        return [{
            "type": "cond",
            "on": cond_name,
            "then": python_to_spec_seq(stmt.body, EnvStack2(dep_set)),
            "else": python_to_spec_seq(stmt.orelse, EnvStack2(dep_set))
        }]
    elif isinstance(stmt, Return):
        ret = stmt.value
        return [{
            "type": "ret",
            "var": ret_key.value,
            "obj": python_ast_to_term(dep_set, ret_val)
        } for ret_key, ret_val in zip(ret.keys, ret.values)]
            
    else:
        targets = stmt.targets
        name = targets[0].id
        app = stmt.value
        fqfunc = app.func
        keywords = {
            **{keyword.arg: keyword.value for keyword in app.keywords},
            **{i: value for i, value in enumerate(app.args)}
        }
        func = fqfunc.attr
        def to_mod(value):
            if isinstance(value, Name):
                return value.id
            else:
                return f"{to_mod(value.value)}.{value.attr}"
        mod = to_mod(fqfunc.value)
        logger.info(f"dep_set = {dep_set}")
        params = {k: python_ast_to_arg(v) for k, v in keywords.items() if not isinstance(v, Name) or v.id not in dep_set}
        dependencies = {k: {
            "depends_on": v.id
        } for k, v in keywords.items() if isinstance(v, Name) and v.id in dep_set}
        
        return [{
            "type": "python",
            "name": name,
            "mod": mod,
            "func": func,
            "params": {**params, **dependencies}
        }]
        


        
        
    
