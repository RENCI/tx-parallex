from queue import Queue
from uuid import uuid4
from random import choice
from enum import Enum
from importlib import import_module
from more_itertools import roundrobin
from autorepr import autorepr, autotext
from multiprocessing import Manager
from ast import parse, Call, Name, UnaryOp, Constant, List, Dict, Return, For, Assign
import logging
from tx.functional.either import Left, Right, Either
from .dependentqueue import DependentQueue, SubQueue


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
    assigns = [stmt for stmt in body if not isinstance(stmt, Return) and not isinstance(stmt, For) and not isinstance(stmt.value, Call)]
    rest = [stmt for stmt in body if stmt not in assigns]
    spec = func(rest)
    if len(assigns) == 0:
        return spec
    else:
        return {
            "type": "let",
            "obj": {
                assign.targets[0].id: python_ast_to_value(assign.value) for assign in assigns
            },
            "sub": spec
        }

    
def python_to_spec(py):
    t = parse(py)
    body = t.body
    
    return python_to_spec_seq(body)
    
    
def python_to_spec_seq(body):
    def func(stmts):
        fors = [stmt for stmt in stmts if isinstance(stmt, For)]
        if len(fors) > 1:
            raise RuntimeError("too many fors")
        elif len(fors) == 1:
            if len(stmts) > 1:
                raise RuntimeError("for cannot be on the same level as return or dependency")
            else:
                return {
                    "type": "map",
                    "var": fors[0].target.id,
                    "coll": {
                        "name": fors[0].iter.id
                    } if isinstance(fors[0].iter, Name) else {
                        "data": python_ast_to_value(fors[0].iter)
                    },
                    "sub": python_to_spec_seq(fors[0].body)
                }
        else:
            apps = [stmt for stmt in stmts if isinstance(stmt, Assign) and isinstance(stmt.value, Call)]
            returns = [stmt for stmt in stmts if isinstance(stmt, Return)]
            dep_set = set(app.targets[0].id for app in apps)
            if len(returns) >= 1:
                ret = returns[0].value
                ret_dict = {python_ast_to_value(k): v.id for k, v in zip(ret.keys, ret.values)}
            else:
                ret_dict = {}
    
            return python_to_top_spec(apps, ret_dict, dep_set)
    
    return let_spec(body, func)


def python_to_top_spec(body, ret_dict, dep_set):
    return {
        "type": "top",
        "sub": [python_to_spec_in_top(stmt, ret_dict, dep_set) for stmt in body]
    }


def python_to_spec_in_top(stmt, ret_dict, dep_set):
    targets = stmt.targets
    name = targets[0].id
    ret = [k for k, v in ret_dict.items() if v == name]
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
    params = {k: python_ast_to_arg(v) for k, v in keywords.items() if not isinstance(v, Name) or v.id not in dep_set}
    dependencies = {k: {
        "depends_on": v.id
    } for k, v in keywords.items() if isinstance(v, Name) and v.id in dep_set}

    return {
        "type": "python",
        "name": name,
        "mod": mod,
        "func": func,
        "params": {**params, **dependencies},
        "ret": ret
    }
        


        
        
    
