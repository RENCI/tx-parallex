from importlib import import_module
from itertools import chain
from ast import parse, Call, Name, UnaryOp, Constant, List, Dict, Return, For, Assign, If, Load, Store, keyword, Compare, BinOp, BoolOp, Add, Sub, Div, Mult, FloorDiv, Mod, MatMult, BitAnd, BitOr, BitXor, Invert, Not, UAdd, USub, LShift, RShift, And, Or, Eq, NotEq, Lt, Gt, LtE, GtE, Eq, NotEq, In, NotIn, Is, IsNot, ImportFrom, Attribute, IfExp, Subscript, Index, Tuple, Starred, With, Expr, Yield, Pow
import ast
import logging
from importlib import import_module
import builtins
from tx.functional.either import Left, Right, Either
from tx.readable_log import getLogger, format_message
from typing import Optional, Any
import typing as t


logger = getLogger(__name__, logging.INFO)


AbsSpec = t.Dict[Any, Any]


def to_mod(value: ast.expr) -> str:
    if isinstance(value, Name):
        return value.id
    elif isinstance(value, Attribute):
        return f"{to_mod(value.value)}.{value.attr}"
    else:
        raise RuntimeError(f"cannot get module {value}")

    
def python_ast_to_value(expr: Optional[ast.expr]) -> Optional[Any]:
    if expr is None:
        return None
    elif isinstance(expr, List):
        return [python_ast_to_value(elt) for elt in expr.elts]
    elif isinstance(expr, Dict):
        return {python_ast_to_value(k): python_ast_to_value(v) for k, v in zip(expr.keys, expr.values)}
    elif isinstance(expr, Constant):
        return expr.value
    else:
        raise RuntimeError(f"cannot convert ast {expr} to value")

    
def python_ast_to_term(expr: ast.expr) -> t.Dict[str, Any]:
    if isinstance(expr, Name):
        return {
            "name": expr.id
        }
    else:
        return {
            "data": python_ast_to_value(expr)
        }

    
# extract expression in for, if, parameters into assignment
def extract_expressions_to_assignments(stmts: t.List[ast.stmt], counter: t.List[Any]=[]) -> t.List[ast.stmt]:
    return list(chain(*(extract_expressions_to_assignments_in_statement(stmt, counter + [i]) for i, stmt in enumerate(stmts))))


def extract_assignments_from_destructure(target: ast.expr, value: ast.expr, type_comments: Optional[str], counter: t.List[Any]) -> t.List[ast.stmt]:
    if isinstance(target, Name):
        return [Assign([target], value, type_comments)]
    elif isinstance(target, Tuple):
        name = generate_variable_name(counter)
        return [Assign([Name(id=name, ctx=Store())], value), *chain(*(extract_assignments_from_destructure(elt, Subscript(value=Name(id=name, ctx=Load()), slice=Index(value=Constant(value=i)), ctx=Load()), None, counter + [i]) for i, elt in enumerate(target.elts)))]
    else:
        raise RuntimeError(f"unsupported assignment to {target}")
    
    
# assuming that var with name _var[x] is not used
# todo: check for var name
def extract_expressions_to_assignments_in_statement(stmt: ast.stmt, counter: t.List[Any]) -> t.List[ast.stmt]:
    if isinstance(stmt, ImportFrom):
        return [stmt]
    if isinstance(stmt, For):
        expr, assigns = extract_expressions_to_assignments_in_expression(stmt.iter, counter)
        stmt_eta: ast.stmt = For(target=stmt.target, iter=expr, body=extract_expressions_to_assignments(stmt.body, counter), orelse=stmt.orelse, type_comment=stmt.type_comment)
        return assigns + [stmt_eta]
    elif isinstance(stmt, If):
        expr, assigns = extract_expressions_to_assignments_in_expression(stmt.test, counter)
        stmt_eta = If(test=expr, body=extract_expressions_to_assignments(stmt.body, counter), orelse=extract_expressions_to_assignments(stmt.orelse, counter))
        return assigns + [stmt_eta]
    if isinstance(stmt, With):
        stmt_eta = With(items=stmt.items, body=extract_expressions_to_assignments(stmt.body, counter), type_comment=stmt.type_comment)
        return [stmt_eta]
    elif isinstance(stmt, Expr) and isinstance(stmt.value, Yield) and stmt.value.value is not None:
        expr, assigns = extract_expressions_to_assignments_in_expression(stmt.value.value, counter)
        stmt_eta = Expr(value=Yield(value=expr))
        return assigns + [stmt_eta]
    elif isinstance(stmt, Assign):
        expr, assigns = extract_expressions_to_assignments_in_expression(stmt.value, counter, in_assignment=True)
        stmt_etas = extract_assignments_from_destructure(stmt.targets[0], expr, stmt.type_comment, counter + ["target"])
        return assigns + stmt_etas
    else:
        raise RuntimeError(f"cannot extract expression from {stmt}")
    
    
def extract_expressions_to_assignments_in_expression(expr: ast.expr, counter: t.List[Any], in_assignment: bool = False) -> t.Tuple[ast.expr, t.List[ast.stmt]]:
    if not is_dynamic(expr):
        return expr, []
    elif isinstance(expr, Name):
        return expr, []
    elif isinstance(expr, Call):
        args, assign_lists = extract_expressions_to_assignments_in_expressions(expr.args, counter)
        assigns = list(chain(*assign_lists))
        keywords_exprs, assign_lists_keywords = extract_expressions_to_assignments_in_expressions([(kw.arg, kw.value) for kw in expr.keywords], counter, keyword=True)
        assigns_keywords = list(chain(*assign_lists_keywords))
        expr_eta: ast.expr = Call(func=expr.func, ctx=Load(), args=list(args), keywords=[keyword(arg=kw.arg, value=kw_expr) for kw, kw_expr in zip(expr.keywords, keywords_exprs)])
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
    elif isinstance(expr, Subscript) and isinstance(expr.slice, Index):
        value, assigns_value = extract_expressions_to_assignments_in_expression(expr.value, counter + ["value"])
        slice, assigns_slice = extract_expressions_to_assignments_in_expression(expr.slice.value, counter + ["slice.value"])
        expr_eta = Subscript(value=value, slice=Index(value=slice), ctx=expr.ctx)
        assigns = assigns_value + assigns_slice
    elif isinstance(expr, Starred):
        value, assigns = extract_expressions_to_assignments_in_expression(expr.value, counter)
        expr_eta = Starred(value=value, ctx=expr.ctx)
    elif isinstance(expr, List):
        exprs, assign_lists = extract_expressions_to_assignments_in_expressions(expr.elts, counter)
        assigns = list(chain(*assign_lists))
        expr_eta = List(elts=list(exprs), ctx=expr.ctx)
    elif isinstance(expr, Tuple):
        exprs, assign_lists = extract_expressions_to_assignments_in_expressions(expr.elts, counter)
        assigns = list(chain(*assign_lists))
        expr_eta = Tuple(elts=list(exprs), ctx=expr.ctx)
    elif isinstance(expr, Dict):
        exprs_keys, assign_keys = extract_expressions_to_assignments_in_expressions(expr.keys, counter + ["keys"])
        exprs_values, assign_values = extract_expressions_to_assignments_in_expressions(expr.values, counter + ["values"])
        assigns = list(chain(*assign_keys, *assign_values))
        expr_eta = Dict(keys=list(exprs_keys), values=list(exprs_values))
    else:
        return expr, []

    logger.debug(format_message("extract_expressions_to_assignments", lambda: ast.dump(expr), {"expression": lambda: ast.dump(expr_eta), "assignments": lambda: [ast.dump(assign) for assign in assigns]}))
    if in_assignment:
#       logger.debug("in assignment")
        return expr_eta, assigns
    else:
#       logger.debug("out of assignment")
        expr, assign = generate_expression_and_assignment(expr_eta, counter)
        return expr, assigns + [assign]
            

def extract_expressions_to_assignments_in_expressions(exprs: t.List[Any], counter: t.List[Any], in_assignment:bool=False, keyword:bool=False) -> t.Tuple[t.Tuple[ast.expr, ...], t.Tuple[t.List[ast.stmt], ...]]:
    if len(exprs) == 0:
            return (), ()
    else:
        if keyword:
            return zip(*(extract_expressions_to_assignments_in_expression(expr, counter + [i], in_assignment=in_assignment) for i, expr in exprs))
        else:
            return zip(*(extract_expressions_to_assignments_in_expression(expr, counter + [i], in_assignment=in_assignment) for i, expr in enumerate(exprs)))

        
def is_dynamic_args(exprs: t.List[ast.expr]) -> bool:
    return any(is_dynamic(expr) for expr in exprs)


def is_dynamic(expr: ast.expr) -> bool:
    return isinstance(expr, Name) or isinstance(expr, Call) or isinstance(expr, BinOp) or isinstance(expr, BoolOp) or isinstance(expr, UnaryOp) or isinstance(expr, Compare) or isinstance(expr, IfExp) or isinstance(expr, Subscript) or (isinstance(expr, List) and is_dynamic_args(expr.elts)) or (isinstance(expr, Tuple) and is_dynamic_args(expr.elts)) or (isinstance(expr, Dict) and (is_dynamic_args(expr.keys) or is_dynamic_args(expr.values))) or (isinstance(expr, Starred) and is_dynamic(expr.value))


def generate_expression_and_assignment(expr_eta: ast.expr, counter: t.List[Any]) -> t.Tuple[Name, Assign]:
    a = generate_variable_name(counter)
    return Name(id=a, ctx=Load()), Assign(targets=[Name(id=a, ctx=Store())], value=expr_eta, type_comment=None)

def generate_variable_name(counter: t.List[str]) -> str:
    return f"_var_{'_'.join(map(str, counter))}"

def canonicalize(stmt: ast.stmt) -> ast.stmt:
    if isinstance(stmt, ImportFrom):
        return stmt
    if isinstance(stmt, For):
        return For(target=stmt.target, iter=stmt.iter, body=canonicalize_list(stmt.body), orelse=stmt.orelse, type_comment=stmt.type_comment)
    elif isinstance(stmt, If):
        return If(test=stmt.test, body=canonicalize_list(stmt.body), orelse=canonicalize_list(stmt.orelse))
    if isinstance(stmt, With):
        return With(items=stmt.items, body=canonicalize_list(stmt.body), type_comment=stmt.type_comment)
    elif isinstance(stmt, Return):
        return Expr(value=Yield(value=stmt.value))
    elif isinstance(stmt, Expr) and isinstance(stmt.value, Yield):
        return stmt
    else:
        return stmt
    
    
def canonicalize_list(stmts: t.List[ast.stmt]) -> t.List[ast.stmt]:
    return list(map(canonicalize, stmts))


def python_to_spec(py: str) -> AbsSpec:
    t = parse(py)
    body = t.body

    body_canonical = canonicalize_list(body)
    body_eta = extract_expressions_to_assignments(body_canonical)
    
    return python_to_spec_seq(body_eta)
    

def python_to_spec_seq(stmts: t.List[ast.stmt], imported_names: t.Dict[str, str] = {}) -> AbsSpec:
    importfroms = [stmt for stmt in stmts if isinstance(stmt, ImportFrom)]
    imported_names2 = {**imported_names, **{func : "" for func in dir(builtins)}, **{func : modname for importfrom in importfroms if (modname := importfrom.module) if (mod := import_module(modname)) if (names := importfrom.names) for func in (dir(mod) if any(x.name == "*" for x in names) else [alias.name for alias in names])}}
    logger.debug(f"imported_names2 = {imported_names2}")
    assigns: t.List[ast.stmt] = [stmt for stmt in stmts if isinstance(stmt, Assign)]
    fors: t.List[ast.stmt] = [stmt for stmt in stmts if isinstance(stmt, For)]
    ifs: t.List[ast.stmt] = [stmt for stmt in stmts if isinstance(stmt, If)]
    withs: t.List[ast.stmt] = [stmt for stmt in stmts if isinstance(stmt, With)]
    returns: t.List[ast.stmt] = [stmt for stmt in stmts if isinstance(stmt, Expr) and isinstance(stmt.value, Yield)]
    
    return python_to_top_spec(assigns + fors + ifs + withs + returns, imported_names2)


def python_to_top_spec(body: t.List[ast.stmt], imported_names: t.Dict[str, str]) -> AbsSpec:
    specs = list(chain.from_iterable(python_to_spec_in_top(stmt, imported_names) for stmt in body))
    if len(specs) == 1:
        return specs[0]
    else:
        return {
            "type": "top",
            "sub": specs
        }

def python_to_spec_in_top(stmt: ast.stmt, imported_names: t.Dict[str, str]) -> t.List[AbsSpec]:
    if isinstance(stmt, For) and isinstance(stmt.target, Name):
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
    elif isinstance(stmt, With):
        return [{
            "type": "seq",
            "sub": list(chain.from_iterable(python_to_spec_in_top(stmt, imported_names) for stmt in stmt.body))
        }]
    elif isinstance(stmt, Expr) and isinstance(stmt.value, Yield):
        ret_val = stmt.value.value
        return [{
            "type": "ret",
            "obj": python_ast_to_term(ret_val)
        }]
            
    elif isinstance(stmt, Assign):
        targets = stmt.targets
        target = targets[0]
        name = target.id
        app = stmt.value
        if not is_dynamic(app):
            return [{
                "type": "let",
                "name": name,
                "obj": python_ast_to_term(app)
            }]
        elif isinstance(app, Name):
            return [{
                "type": "let",
                "name": name,
                "obj": python_ast_to_term(app)
            }]
        elif isinstance(app, Starred):
            func = "_starred"
            mod = "tx.parallex.data"
            keywords: t.Dict[Any, Any] = {
                0: app.value
            }
        elif isinstance(app, Call):
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
            bop = app.op
            keywords = {
                0: app.left,
                1: app.right
            }
            mod = "tx.parallex.data"
            if isinstance(bop, Add):
                func = "_add"
            elif isinstance(bop, Sub):
                func = "_sub"
            elif isinstance(bop, Mult):
                func = "_mult"
            elif isinstance(bop, Div):
                func = "_div"
            elif isinstance(bop, MatMult):
                func = "_mat_mult"
            elif isinstance(bop, Mod):
                func = "_mod"
            elif isinstance(bop, Pow):
                func = "_pow"
            elif isinstance(bop, LShift):
                func = "_l_shift"
            elif isinstance(bop, RShift):
                func = "_r_shift"
            elif isinstance(bop, BitOr):
                func = "_bit_or"
            elif isinstance(bop, BitXor):
                func = "_bit_xor"
            elif isinstance(bop, BitAnd):
                func = "_bit_and"
            elif isinstance(bop, FloorDiv):
                func = "_floor_div"
                
        elif isinstance(app, UnaryOp):
            unaop = app.op
            keywords = {
                0: app.operand
            }
            mod = "tx.parallex.data"
            if isinstance(unaop, Invert):
                func = "_invert"
            elif isinstance(unaop, Not):
                func = "_not"
            elif isinstance(unaop, UAdd):
                func = "_u_add"
            elif isinstance(unaop, USub):
                func = "_u_sub"
                
        elif isinstance(app, BoolOp):
            boolop = app.op
            keywords = {i: value for i, value in enumerate(app.values)}
            mod = "tx.parallex.data"
            if isinstance(boolop, And):
                func = "_and"
            elif isinstance(boolop, Or):
                func = "_or"

        elif isinstance(app, IfExp):
            keywords = {
                0: app.test,
                1: app.body,
                2: app.orelse
            }
            mod = "tx.parallex.data"
            func = "_if_exp"

        elif isinstance(app, Subscript) and isinstance(app.slice, Index):
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
    else:
        raise RuntimeError(f"error {app}")    



