from queue import Queue
from uuid import uuid4
from random import choice
from enum import Enum
from importlib import import_module
from more_itertools import roundrobin
from itertools import chain
from autorepr import autorepr, autotext
from multiprocessing import Manager
from ast import parse, Call, Name, UnaryOp, Constant, List, Dict, Return, For, Assign, If, Load, Store, keyword, Compare, BinOp, BoolOp, Add, Sub, Div, Mult, FloorDiv, Mod, MatMult, BitAnd, BitOr, BitXor, Invert, Not, UAdd, USub, LShift, RShift, And, Or, Eq, NotEq, Lt, Gt, LtE, GtE, Eq, NotEq, In, NotIn, Is, IsNot, ImportFrom, Attribute, IfExp, Subscript, Index, Tuple
import ast
import logging
from importlib import import_module
import builtins
from tx.functional.either import Left, Right, Either
from .dependentqueue import DependentQueue, SubQueue
from .stack import Stack
from tx.readable_log import getLogger


logger = getLogger(__name__, logging.INFO)


def to_mod(value):
    if isinstance(value, Name):
        return value.id
    else:
        return f"{to_mod(value.value)}.{value.attr}"

    
def python_ast_to_value(expr):
    if isinstance(expr, List):
        return [python_ast_to_value(elt) for elt in expr.elts]
    elif isinstance(expr, Dict):
        return {python_ast_to_value(k): python_ast_to_value(v) for k, v in zip(expr.keys, expr.values)}
    elif isinstance(expr, Constant):
        return expr.value
    else:
        raise RuntimeError(f"cannot convert ast {expr} to value")

    
def python_ast_to_term(expr):
    if isinstance(expr, Name):
        return {
            "name": expr.id
        }
    else:
        return {
            "data": python_ast_to_value(expr)
        }

    
def let_spec(body, func):
    assigns = [stmt for stmt in body if isinstance(stmt, Assign) and not is_dynamic(stmt.value)]
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
                "obj": python_ast_to_term(assign.value),
                "sub": spec
            }
            return let_spec_handle_assign(resta, spec_wrapped_by_assign)
    return let_spec_handle_assign(assigns, spec)

# extract expression in for, if, parameters into assignment
def extract_expressions_to_assignments(stmts, counter=[]):
    return list(chain(*(extract_expressions_to_assignments_in_statement(stmt, counter + [i]) for i, stmt in enumerate(stmts))))


def extract_assignments_from_destructure(target, value, type_comments, counter):
    if isinstance(target, Name):
        return [Assign([target], value, type_comments)]
    elif isinstance(target, Tuple):
        name = generate_variable_name(counter)
        return [Assign([Name(id=name, ctx=Store())], value)] + list(chain(*(extract_assignments_from_destructure(elt, Subscript(value=Name(id=name, ctx=Load()), slice=Index(value=Constant(value=i)), ctx=Load()), None, counter + [i]) for i, elt in enumerate(target.elts))))
    else:
        raise RuntimeError(f"unsupported assignment to {target}")
    
    
# assuming that var with name _var[x] is not used
# todo: check for var name
def extract_expressions_to_assignments_in_statement(stmt, counter):
    if isinstance(stmt, ImportFrom):
        return [stmt]
    if isinstance(stmt, For):
        expr, assigns = extract_expressions_to_assignments_in_expression(stmt.iter, counter)
        stmt_eta = For(target=stmt.target, iter=expr, body=extract_expressions_to_assignments(stmt.body, counter), orelse=stmt.orelse, type_comment=stmt.type_comment)
        return assigns + [stmt_eta]
    elif isinstance(stmt, If):
        expr, assigns = extract_expressions_to_assignments_in_expression(stmt.test, counter)
        stmt_eta = If(test=expr, body=extract_expressions_to_assignments(stmt.body, counter), orelse=extract_expressions_to_assignments(stmt.orelse, counter))
        return assigns + [stmt_eta]
    elif isinstance(stmt, Return):
        ret = stmt.value
        exprs, assigns = zip(*(extract_expressions_to_assignments_in_expression(value, counter) for value in ret.values))
        stmt_eta = Return(value=Dict(keys=ret.keys, values=list(exprs)))
        return list(chain(*assigns)) + [stmt_eta]
    else:
        expr, assigns = extract_expressions_to_assignments_in_expression(stmt.value, counter, in_assignment=True)
        stmt_eta = extract_assignments_from_destructure(stmt.targets[0], expr, stmt.type_comment, counter + ["target"])
        return assigns + stmt_eta
    
    
def extract_expressions_to_assignments_in_expression(expr, counter, in_assignment=False):
    if isinstance(expr, Call):
        args, assign_lists = extract_expressions_to_assignments_in_expressions(expr.args, counter)
        assigns = list(chain(*assign_lists))
        keywords_exprs, assign_lists_keywords = extract_expressions_to_assignments_in_expressions([(kw.arg, kw.value) for kw in expr.keywords], counter, keyword=True)
        assigns_keywords = list(chain(*assign_lists_keywords))
        expr_eta = Call(func=expr.func, ctx=Load(), args=list(args), keywords=[keyword(arg=kw.arg, value=kw_expr) for kw, kw_expr in zip(expr.keywords, keywords_exprs)])
        assigns = assigns + assigns_keywords
    elif isinstance(expr, Compare):
        left, assigns_left = extract_expressions_to_assignments_in_expression(expr.left, counter + ["left"])
        comparators, assign_lists = extract_expressions_to_assignments_in_expressions(expr.comparators, counter + ["comparator"])
        expr_eta = Compare(left=left, ops=expr.ops, comparators=list(comparators))
        assigns = assigns_left + list(chain(*assign_lists))
    elif isinstance(expr, BinOp):
        left, assigns_left = extract_expressions_to_assignments_in_expression(expr.left, counter + ["left"])
        right, assigns_right = extract_expressions_to_assignments_in_expression(expr.right, counter + ["right"])
        expr_eta = BinOp(left=left, op=expr.op, right=right)
        assigns = assigns_left + assigns_right
    elif isinstance(expr, BoolOp):
        values, assign_lists = extract_expressions_to_assignments_in_expressions(expr.values, counter)
        assigns = list(chain(*assign_lists))
        expr_eta = BoolOp(op=expr.op, values=list(values))
    elif isinstance(expr, UnaryOp):
        operand, assigns = extract_expressions_to_assignments_in_expression(expr.operand, counter)
        expr_eta = UnaryOp(op=expr.op, operand=operand)
    elif isinstance(expr, IfExp):
        test, assigns_test = extract_expressions_to_assignments_in_expression(expr.test, counter + ["test"])
        body, assigns_body = extract_expressions_to_assignments_in_expression(expr.body, counter + ["body"])
        orelse, assigns_orelse = extract_expressions_to_assignments_in_expression(expr.orelse, counter + ["orelse"])
        expr_eta = IfExp(test=test, body=body, orelse=orelse)
        assigns = assigns_test + assigns_body + assigns_orelse
    elif isinstance(expr, Subscript):
        value, assigns_value = extract_expressions_to_assignments_in_expression(expr.value, counter + ["value"])
        slice, assigns_slice = extract_expressions_to_assignments_in_expression(expr.slice.value, counter + ["slice.value"])
        expr_eta = Subscript(value=value, slice=Index(value=slice), ctx=expr.ctx)
        assigns = assigns_value + assigns_slice
    elif isinstance(expr, List):
        exprs, assign_lists = extract_expressions_to_assignments_in_expressions(expr.elts, counter)
        assigns = list(chain(*assign_lists))
        if not is_dynamic_args(exprs) and len(assigns) == 0:
            return expr, []
        else:
            expr_eta = List(elts=list(exprs), ctx=expr.ctx)
    elif isinstance(expr, Tuple):
        exprs, assign_lists = extract_expressions_to_assignments_in_expressions(expr.elts, counter)
        assigns = list(chain(*assign_lists))
        if not is_dynamic_args(exprs) and len(assigns) == 0:
            return expr, []
        else:
            expr_eta = Tuple(elts=list(exprs), ctx=expr.ctx)
    elif isinstance(expr, Dict):
        exprs_keys, assign_keys = extract_expressions_to_assignments_in_expressions(expr.keys, counter + ["keys"])
        exprs_values, assign_values = extract_expressions_to_assignments_in_expressions(expr.values, counter + ["values"])
        assigns = list(chain(*assign_keys, *assign_values))
        if not is_dynamic_args(exprs_keys) and not is_dynamic_args(exprs_values) and len(assigns) == 0:
            return expr, []
        else:
            expr_eta = Dict(keys=list(exprs_keys), values=list(exprs_values))
    else:
        return expr, []

    logger.info(f"{ast.dump(expr_eta)} {[ast.dump(assign) for assign in assigns]}")
    if in_assignment:
#       logger.debug("in assignment")
        return expr_eta, assigns
    else:
#       logger.debug("out of assignment")
        expr, assign = generate_expression_and_assignment(expr_eta, counter)
        return expr, assigns + [assign]
            

def extract_expressions_to_assignments_in_expressions(exprs, counter, in_assignment=False, keyword=False):
    if len(exprs) == 0:
            return exprs, []
    else:
        if keyword:
            return zip(*(extract_expressions_to_assignments_in_expression(expr, counter + [i], in_assignment=in_assignment) for i, expr in exprs))
        else:
            return zip(*(extract_expressions_to_assignments_in_expression(expr, counter + [i], in_assignment=in_assignment) for i, expr in enumerate(exprs)))
    
    
def is_dynamic_args(exprs):
    return any(isinstance(expr, Name) for expr in exprs)

def generate_expression_and_assignment(expr_eta, counter):
    a = generate_variable_name(counter)
    return Name(id=a, ctx=Load()), Assign(targets=[Name(id=a, ctx=Store())], value=expr_eta, type_comment=None)

def generate_variable_name(counter):
    return f"_var_{'_'.join(map(str, counter))}"


def python_to_spec(py):
    t = parse(py)
    body = t.body

    body_eta = extract_expressions_to_assignments(body)
    
    return python_to_spec_seq(body_eta)
    

def is_dynamic(stmt):
    return isinstance(stmt, Call) or isinstance(stmt, BinOp) or isinstance(stmt, BoolOp) or isinstance(stmt, UnaryOp) or isinstance(stmt, Compare) or isinstance(stmt, IfExp) or isinstance(stmt, Subscript) or (isinstance(stmt, List) and is_dynamic_args(stmt.elts)) or (isinstance(stmt, Tuple) and is_dynamic_args(stmt.elts)) or (isinstance(stmt, Dict) and (is_dynamic_args(stmt.keys) or is_dynamic_args(stmt.values)))


def python_to_spec_seq(body, imported_names = {}):
    def func(stmts):
        importfroms = [stmt for stmt in stmts if isinstance(stmt, ImportFrom)]
        imported_names2 = {**imported_names, **{func : "" for func in dir(builtins)}, **{func : modname for importfrom in importfroms if (modname := importfrom.module) if (mod := import_module(modname)) if (names := importfrom.names) for func in (dir(mod) if any(x.name == "*" for x in names) else [alias.name for alias in names])}}
        logger.debug(f"imported_names2 = {imported_names2}")
        apps = [stmt for stmt in stmts if isinstance(stmt, Assign) and is_dynamic(stmt.value)]
        fors = [stmt for stmt in stmts if isinstance(stmt, For)]
        ifs = [stmt for stmt in stmts if isinstance(stmt, If)]
        returns = [stmt for stmt in stmts if isinstance(stmt, Return)]
    
        return python_to_top_spec(apps + fors + ifs + returns, imported_names2)
    
    return let_spec(body, func)


def python_to_top_spec(body, imported_names):
    specs = list(chain.from_iterable(python_to_spec_in_top(stmt, imported_names) for stmt in body))
    if len(specs) == 1:
        return specs[0]
    else:
        return {
            "type": "top",
            "sub": specs
        }

EnvStack2 = Stack(set())

def python_to_spec_in_top(stmt, imported_names):
    if isinstance(stmt, For):
        coll_name = python_ast_to_term(stmt.iter)
        return [{
            "type": "map",
            "var": stmt.target.id,
            "coll": coll_name,
            "sub": python_to_spec_seq(stmt.body, imported_names)
        }]
    elif isinstance(stmt, If):
        cond_name = python_ast_to_term(stmt.test)
        return [{
            "type": "cond",
            "on": cond_name,
            "then": python_to_spec_seq(stmt.body, imported_names),
            "else": python_to_spec_seq(stmt.orelse, imported_names),
        }]
    elif isinstance(stmt, Return):
        ret = stmt.value
        return [{
            "type": "ret",
            "var": ret_key.value,
            "obj": python_ast_to_term(ret_val)
        } for ret_key, ret_val in zip(ret.keys, ret.values)]
            
    else:
        targets = stmt.targets
        target = targets[0]
        name = target.id
        app = stmt.value
        if isinstance(app, Call):
            fqfunc = app.func
            keywords = {
                **{keyword.arg: keyword.value for keyword in app.keywords},
                **{i: value for i, value in enumerate(app.args)}
            }
            if isinstance(fqfunc, Attribute):
                func = fqfunc.attr
                mod = to_mod(fqfunc.value)
            else:
                func = fqfunc.id
                mod = imported_names[func]
            # logger.debug(f"dep_set = {dep_set}")
        elif isinstance(app, Compare):
            if len(app.ops) > 1:
                raise RuntimeError("unsupported multi-op compare")
            op = app.ops[0]
            keywords = {
                0: app.left,
                1: app.comparators[0]
            }
            mod = "tx.parallex.data"
            if isinstance(op, Eq):
                func = "_eq"
            elif isinstance(op, NotEq):
                func = "_not_eq"
            elif isinstance(op, Lt):
                func = "_lt"
            elif isinstance(op, LtE):
                func = "_lt_e"
            elif isinstance(op, Gt):
                func = "_gt"
            elif isinstance(op, GtE):
                func = "_gt_e"
            elif isinstance(op, Is):
                func = "_is"
            elif isinstance(op, IsNot):
                func = "_is_not"
            elif isinstance(op, In):
                func = "_in"
            elif isinstance(op, NotIn):
                func = "_not_in"
                
        elif isinstance(app, BinOp):
            op = app.op
            keywords = {
                0: app.left,
                1: app.right
            }
            mod = "tx.parallex.data"
            if isinstance(op, Add):
                func = "_add"
            elif isinstance(op, Sub):
                func = "_sub"
            elif isinstance(op, Mult):
                func = "_mult"
            elif isinstance(op, Div):
                func = "_div"
            elif isinstance(op, MatMult):
                func = "_mat_mult"
            elif isinstance(op, Mod):
                func = "_mod"
            elif isinstance(op, Pow):
                func = "_pow"
            elif isinstance(op, LShift):
                func = "_l_shift"
            elif isinstance(op, RShift):
                func = "_r_shift"
            elif isinstance(op, BitOr):
                func = "_bit_or"
            elif isinstance(op, BitXor):
                func = "_bit_xor"
            elif isinstance(op, BitAnd):
                func = "_bit_and"
            elif isinstance(op, FloorDiv):
                func = "_floor_div"
                
        elif isinstance(app, UnaryOp):
            op = app.op
            keywords = {
                0: app.operand
            }
            mod = "tx.parallex.data"
            if isinstance(op, Invert):
                func = "_invert"
            elif isinstance(op, Not):
                func = "_not"
            elif isinstance(op, UAdd):
                func = "_u_add"
            elif isinstance(op, USub):
                func = "_u_sub"
                
        elif isinstance(app, BoolOp):
            op = app.op
            keywords = {i: value for i, value in enumerate(app.values)}
            mod = "tx.parallex.data"
            if isinstance(op, And):
                func = "_and"
            elif isinstance(op, Or):
                func = "_or"

        elif isinstance(app, IfExp):
            keywords = {
                0: app.test,
                1: app.body,
                2: app.orelse
            }
            mod = "tx.parallex.data"
            func = "_if_exp"

        elif isinstance(app, Subscript):
            keywords = {
                0: app.value,
                1: app.slice.value
            }
            mod = "tx.parallex.data"
            func = "_subscript"
                
        elif isinstance(app, List):
            keywords = {i: elt for i, elt in enumerate(app.elts)}
            mod = "tx.parallex.data"
            func = "_list"
                
        elif isinstance(app, Tuple):
            keywords = {i: elt for i, elt in enumerate(app.elts)}
            mod = "tx.parallex.data"
            func = "_tuple"
                
        elif isinstance(app, Dict):
            keywords = {i: elt for i, elt in enumerate(app.keys + app.values)}
            mod = "tx.parallex.data"
            func = "_dict"
                
        params = {k: python_ast_to_term(v) for k, v in keywords.items()}
        
        return [{
            "type": "python",
            "name": name,
            "mod": mod,
            "func": func,
            "params": params
        }]



