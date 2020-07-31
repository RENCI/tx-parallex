import ast
import pytest
from tx.parallex.python import python_to_spec, extract_expressions_to_assignments
from tx.functional.either import Left, Right

def test_python_to_spec1():
    py = "a = mod1.mod2.func(param=arg)"
    spec = python_to_spec(py)
    assert spec == {
        "type": "python",
        "name": "a",
        "mod": "mod1.mod2",
        "func": "func",
        "params": {
            "param": {
                "name": "arg"
            }
        }
    }


def test_python_to_spec2():
    py = """
var = mod3.func2()
a = mod1.mod2.func(param=var)"""
    spec = python_to_spec(py)
    assert spec == {
        "type":"top",
        "sub": [{
            "type": "python",
            "name": "var",
            "mod": "mod3",
            "func": "func2",
            "params": {}
        }, {
            "type": "python",
            "name": "a",
            "mod": "mod1.mod2",
            "func": "func",
            "params": {
                "param": {
                    "name": "var"
                }
            }
        }]       
    }

def test_python_to_spec3():
    py = """
var = mod3.func2()
a = mod1.mod2.func(param=var)
return {'ret1': a}"""
    spec = python_to_spec(py)
    assert spec == {
        "type":"top",
        "sub": [{
            "type": "python",
            "name": "var",
            "mod": "mod3",
            "func": "func2",
            "params": {}
        }, {
            "type": "python",
            "name": "a",
            "mod": "mod1.mod2",
            "func": "func",
            "params": {
                "param": {
                    "name": "var"
                }
            }
        }, {
            "type": "ret",
            "var": "ret1",
            "obj": {
                "name": "a"
            }
        }]
    }

def test_python_to_spec4():
    py = "a = 1"
    spec = python_to_spec(py)
    assert spec == {
        "type":"let",
        "var": "a",
        "obj": {
            "data": 1
        },
        "sub": {
            "type": "top",
            "sub": []
        }
    }


def test_python_to_spec5():
    py = """
for i in c:
    x = mod1.func2(r=i)"""

    spec = python_to_spec(py)
    assert spec == {
        "type": "map",
        "var": "i",
        "coll": {
            "name": "c"
        },
        "sub": {
                "type": "python",
                "name": "x",
                "mod": "mod1",
                "func": "func2",
                "params": {
                    "r": {
                        "name": "i"
                    }
                }
        }
    }

    
def test_python_to_spec6():
    py = """
y = 1
for i in c:
    x = mod1.func2(r=i)"""

    spec = python_to_spec(py)
    assert spec == {
        "type": "let",
        "var": "y",
        "obj": {
            "data": 1
        },
        "sub": {
            "type": "map",
            "var": "i",
            "coll": {
                "name": "c"
            },
            "sub": {
                    "type": "python",
                    "name": "x",
                    "mod": "mod1",
                    "func": "func2",
                    "params": {
                        "r": {
                            "name": "i"
                        }
                    }
            }
        }
    }

def test_python_to_spec7():
    py = """
y = 1
for i in c:
    for j in d:
        x = mod1.func2(s=i,t=j)"""

    spec = python_to_spec(py)
    assert spec == {
        "type": "let",
        "var": "y",
        "obj": {
            "data": 1
        },
        "sub": {
            "type": "map",
            "var": "i",
            "coll": {
                "name": "c"
            },
            "sub": {
                "type": "map",
                "var": "j",
                "coll": {
                    "name": "d"
                },
                "sub": {
                        "type": "python",
                        "name": "x",
                        "mod": "mod1",
                        "func": "func2",
                        "params": {
                            "s": {
                                "name": "i"
                            },
                            "t": {
                                "name": "j"
                            }
                        }
                }
            }
        }
    }

def test_python_to_spec8():
    py = """
y = 4
for i in c:
    z = 390
    for j in d:
        x = mod1.func2(s=i,t=j)"""

    spec = python_to_spec(py)
    assert spec == {
        "type": "let",
        "var": "y",
        "obj": {
            "data": 4
        },
        "sub": {
            "type": "map",
            "var": "i",
            "coll": {
                "name": "c"
            },
            "sub": {
                "type": "let",
                "var": "z",
                "obj": {
                    "data": 390
                },
                "sub": {
                    "type": "map",
                    "var": "j",
                    "coll": {
                        "name": "d"
                    },
                    "sub": {
                            "type": "python",
                            "name": "x",
                            "mod": "mod1",
                            "func": "func2",
                            "params": {
                                "s": {
                                    "name": "i"
                                },
                                "t": {
                                    "name": "j"
                                }
                            }
                    }
                }
            }
        }
    }

def test_python_to_spec9():
    py = """
y = mod2.func3(x)
for i in c:
    x = mod1.func2(u=y)"""

    spec = python_to_spec(py)
    assert spec == {
        "type": "top",
        "sub": [{
            "type": "python",
            "name": "y",
            "mod": "mod2",
            "func": "func3",
            "params": {
                0: {
                    "name": "x"
                }
            }
        }, {
            "type": "map",
            "var": "i",
            "coll": {
                "name": "c"
            },
            "sub": {
                "type": "python",
                "name": "x",
                "mod": "mod1",
                "func": "func2",
                "params": {
                    "u": {
                        "name": "y"
                    }
                }
            }
        }]
    }

def test_python_to_spec10():
    py = """x = mod1.func2(i)"""

    spec = python_to_spec(py)
    assert spec == {
            "type": "python",
            "name": "x",
            "mod": "mod1",
            "func": "func2",
            "params": {
                0: {
                    "name": "i"
                }
            }
    }

def test_python_to_spec11():
    py = """
d = [2,3]
c = tests.test_task.identity(d)
for j in c:
    a = tests.test_task.g(x=2,y=j)
    return {"x": a}"""

    spec = python_to_spec(py)
    assert spec == {
        "type": "let",
        "var": "d",
        "obj": {
            "data": [2,3]
        },
        "sub": {
            "type": "top",
            "sub": [{
                "type": "python",
                "name": "c",
                "mod": "tests.test_task",
                "func": "identity",
                "params": {
                    0: {
                        "name": "d"
                    }
                }
            }, {
                "type": "map",
                "var": "j",
                "coll": {
                    "name": "c"
                },
                "sub": {
                    "type": "top",
                    "sub": [{
                        "type": "python",
                        "name": "a",
                        "mod": "tests.test_task",
                        "func": "g",
                        "params": {
                            "x": {
                                "data": 2
                            },
                            "y": {
                                "name": "j"
                            }
                        }
                    }, {
                        "type": "ret",
                        "var": "x",
                        "obj": {
                            "name": "a"
                        }
                    }]
                }
            }]
        }
    }

def test_python_to_spec12():
    py = """
z = True
if z:
    return {
        "x": 1
    }
else:
    return {
        "x": 0
    }"""

    spec = python_to_spec(py)
    assert spec == {
        "type": "let",
        "var": "z",
        "obj": {
            "data": True
        },
        "sub": {
            "type": "cond",
            "on": {
                "name": "z"
            },
            "then": {
                "type": "ret",
                "var": "x",
                "obj": {
                    "data": 1
                }
            },
            "else": {
                "type": "ret",
                "var": "x",
                "obj": {
                    "data": 0
                }
            }
        }
    }
    

def test_python_to_spec13():
    py = """
z = False
if z:
    return {
        "x": 1
    }
else:
    return {
        "x": 0
    }"""

    spec = python_to_spec(py)
    assert spec == {
        "type": "let",
        "var": "z",
        "obj": {
            "data": False
        },
        "sub": {
            "type": "cond",
            "on": {
                "name": "z"
            },
            "then": {
                "type": "ret",
                "var": "x",
                "obj": {
                    "data": 1
                }
            },
            "else": {
                "type": "ret",
                "var": "x",
                "obj": {
                    "data": 0
                }
            }
        }
    }
    

def test_python_to_spec14():
    py = """
z = tests.test_task.true()
if z:
    return {
        "x": 1
    }
else:
    return {
        "x": 0
    }"""

    spec = python_to_spec(py)
    assert spec == {
        "type": "top",
        "sub": [{
            "type": "python",
            "name": "z",
            "mod": "tests.test_task",
            "func": "true",
            "params": {
            }
        }, {
            "type": "cond",
            "on": {
                "name": "z"
            },
            "then": {
                "type": "ret",
                "var": "x",
                "obj": {
                    "data": 1
                }
            },
            "else": {
                "type": "ret",
                "var": "x",
                "obj": {
                    "data": 0
                }
            }
        }]
    }


def test_python_to_spec15():
    py = """
z = tests.test_task.false()
if z:
    return {
        "x": 1
    }
else:
    return {
        "x": 0
    }"""

    spec = python_to_spec(py)
    assert spec == {
        "type": "top",
        "sub": [{
            "type": "python",
            "name": "z",
            "mod": "tests.test_task",
            "func": "false",
            "params": {
            }
        }, {
            "type": "cond",
            "on": {
                "name": "z"
            },
            "then": {
                "type": "ret",
                "var": "x",
                "obj": {
                    "data": 1
                }
            },
            "else": {
                "type": "ret",
                "var": "x",
                "obj": {
                    "data": 0
                }
            }
        }]
    }

def do_compare_expression(py, py2):
    py_ast = ast.parse(py)
    py_ast_dump = ast.dump(ast.Module(body=extract_expressions_to_assignments(py_ast.body), type_ignores=py_ast.type_ignores))
    ast2 = ast.dump(ast.parse(py2))
    assert py_ast_dump == ast2


def test_expression_to_assigns1():
    py = """
z = f(g())
"""
    py2 = """
_var_0_0 = g()
z = f(_var_0_0)
"""
    do_compare_expression(py, py2)

def test_expression_to_assigns2():
    py = """
z = f(g(c(),d()))
"""
    py2 = """
_var_0_0_0 = c()
_var_0_0_1 = d()
_var_0_0 = g(_var_0_0_0, _var_0_0_1)
z = f(_var_0_0)
"""
    do_compare_expression(py, py2)

def test_expression_to_assigns3():
    py = """
z = f(g(i=c(),j=d()))
"""
    py2 = """
_var_0_0_i = c()
_var_0_0_j = d()
_var_0_0 = g(i=_var_0_0_i, j=_var_0_0_j)
z = f(_var_0_0)
"""
    do_compare_expression(py, py2)

def test_expression_to_assigns4():
    py = """
for i in f():
    return {"i":i}
"""
    py2 = """
_var_0 = f()
for i in _var_0:
    return {"i":i}
"""
    do_compare_expression(py, py2)

def test_expression_to_assigns5():
    py = """
if f():
    return {"i":i}
"""
    py2 = """
_var_0 = f()
if _var_0:
    return {"i":i}
"""
    do_compare_expression(py, py2)

def test_expression_to_assigns6():
    py = """
if a == b:
    return {"i":i}
"""
    py2 = """
_var_0 = a == b
if _var_0:
    return {"i":i}
"""
    do_compare_expression(py, py2)

def test_expression_to_assigns7():
    py = """
if a and b:
    return {"i":i}
"""
    py2 = """
_var_0 = a and b
if _var_0:
    return {"i":i}
"""
    do_compare_expression(py, py2)

def test_expression_to_assigns8():
    py = """
if a in b:
    return {"i":i}
"""
    py2 = """
_var_0 = a in b
if _var_0:
    return {"i":i}
"""
    do_compare_expression(py, py2)

def test_expression_to_assigns9():
    py = """
if c() and d():
    return {"i":i}
"""
    py2 = """
_var_0_0 = c()
_var_0_1 = d()
_var_0 = _var_0_0 and _var_0_1
if _var_0:
    return {"i":i}
"""
    do_compare_expression(py, py2)

def test_expression_to_assigns10():
    py = """
if c() in d():
    return {"i":i}
"""
    py2 = """
_var_0_left = c()
_var_0_comparator_0 = d()
_var_0 = _var_0_left in _var_0_comparator_0
if _var_0:
    return {"i":i}
"""
    do_compare_expression(py, py2)

def test_expression_to_assigns11():
    py = """
a = c() + d()
"""
    py2 = """
_var_0_left = c()
_var_0_right = d()
a = _var_0_left + _var_0_right
"""
    do_compare_expression(py, py2)

def test_expression_to_assigns12():
    py = """
a = c() @ d()
"""
    py2 = """
_var_0_left = c()
_var_0_right = d()
a = _var_0_left @ _var_0_right
"""
    do_compare_expression(py, py2)

def test_expression_to_assigns13():
    py = """
a = ~c()
"""
    py2 = """
_var_0 = c()
a = ~_var_0
"""
    do_compare_expression(py, py2)


def test_expression_to_assigns14():
    py = """
a = c() and d() and e()
"""
    py2 = """
_var_0_0 = c()
_var_0_1 = d()
_var_0_2 = e()
a = _var_0_0 and _var_0_1 and _var_0_2
"""
    do_compare_expression(py, py2)


def test_python_to_spec16():
    py = """
a = 1 + 2
"""

    spec = python_to_spec(py)

    assert spec == {
        "type": "python",
        "name": "a",
        "mod": "tx.parallex.data",
        "func": "_add",
        "params": {
            0: {
                "data": 1
            },
            1: {
                "data": 2
            }
        }
    }


def test_python_to_spec17():
    py = """
a = 1 - 2
"""

    spec = python_to_spec(py)

    assert spec == {
        "type": "python",
        "name": "a",
        "mod": "tx.parallex.data",
        "func": "_sub",
        "params": {
            0: {
                "data": 1
            },
            1: {
                "data": 2
            }
        }
    }


def test_python_to_spec18():
    py = """
a = 1 * 2
"""

    spec = python_to_spec(py)

    assert spec == {
        "type": "python",
        "name": "a",
        "mod": "tx.parallex.data",
        "func": "_mult",
        "params": {
            0: {
                "data": 1
            },
            1: {
                "data": 2
            }
        }
    }


def test_python_to_spec19():
    py = """
a = 1 / 2
"""

    spec = python_to_spec(py)

    assert spec == {
        "type": "python",
        "name": "a",
        "mod": "tx.parallex.data",
        "func": "_div",
        "params": {
            0: {
                "data": 1
            },
            1: {
                "data": 2
            }
        }
    }


def test_python_to_spec20():
    py = """
z = a and b and c
"""

    spec = python_to_spec(py)

    assert spec == {
        "type": "python",
        "name": "z",
        "mod": "tx.parallex.data",
        "func": "_and",
        "params": {
            0: {
                "name": "a"
            },
            1: {
                "name": "b"
            },
            2: {
                "name": "c"
            }
        }
    }


def func(x):
    return x


def test_python_to_spec21():
    py = """
from tests.test_python import func
a = func(param=arg)
"""
    spec = python_to_spec(py)
    assert spec == {
        "type": "python",
        "name": "a",
        "mod": "tests.test_python",
        "func": "func",
        "params": {
            "param": {
                "name": "arg"
            }
        }
    }


def test_python_to_spec21():
    py = """
from tests.test_python import func
for i in a:
    a = func(param=arg)
"""
    spec = python_to_spec(py)
    assert spec == {
        "type": "map",
        "coll": {
            "name": "a"
        },
        "var": "i",
        "sub": {
            "type": "python",
            "name": "a",
            "mod": "tests.test_python",
            "func": "func",
            "params": {
                "param": {
                    "name": "arg"
                }
            }
        }
    }


def test_python_to_spec22():
    py = """
from tests.test_python import *
a = func(param=arg)
"""
    spec = python_to_spec(py)
    assert spec == {
        "type": "python",
        "name": "a",
        "mod": "tests.test_python",
        "func": "func",
        "params": {
            "param": {
                "name": "arg"
            }
        }
    }

def test_python_to_spec23():
    py = """
a = all([True])
"""
    spec = python_to_spec(py)
    assert spec == {
        "type": "python",
        "name": "a",
        "mod": "",
        "func": "all",
        "params": {
            0: {
                "data": [True]
            }
        }
    }

def test_python_to_spec24():
    py = """
a = allx([True])
"""
    with pytest.raises(KeyError):
        spec = python_to_spec(py)


def test_python_to_spec25():
    py = """
c = tests.test_task.identity([0])
for j in c:
    return {"x": j}"""

    spec = python_to_spec(py)
    assert spec == {
        "type": "top",
        "sub": [{
            "type": "python",
            "name": "c",
            "mod": "tests.test_task",
            "func": "identity",
            "params": {
                0: {
                    "data": [0]
                }
            }
        }, {
            "type": "map",
            "coll": {
                "name": "c"
            },
            "var": "j",
            "sub": {
                "type": "ret",
                "var": "x",
                "obj": {
                    "name": "j"
                }
            }
        }]
    }

    
def test_python_to_spec26():
    py = """
c = [1,2,3][0]
"""

    spec = python_to_spec(py)
    assert spec == {
        "type": "python",
        "name": "c",
        "mod": "tx.parallex.data",
        "func": "_subscript",
        "params": {
            0: {
                "data": [1,2,3]
            },
            1: {
                "data": 0
            }
        }
    }

    
def test_python_to_spec27():
    py = """
a,b,c = [1,2,3]
"""

    spec = python_to_spec(py)
    assert spec == {
        "type": "let",
        "var": "_var_0_target",
        "obj": {
            "data": [1,2,3]
        },
        "sub": {
            "type": "top",
            "sub": [{
                "type": "python",
                "name": "a",
                "mod": "tx.parallex.data",
                "func": "_subscript",
                "params": {
                    0: {
                        "name": "_var_0_target"
                    },
                    1: {
                        "data": 0
                    }
                }
            },{
                "type": "python",
                "name": "b",
                "mod": "tx.parallex.data",
                "func": "_subscript",
                "params": {
                    0: {
                        "name": "_var_0_target"
                    },
                    1: {
                        "data": 1
                    }
                }
            },{
                "type": "python",
                "name": "c",
                "mod": "tx.parallex.data",
                "func": "_subscript",
                "params": {
                    0: {
                        "name": "_var_0_target"
                    },
                    1: {
                        "data": 2
                    }
                }
            }]
        }
    }

def test_python_to_spec_dynamic_list():
    py = """
a = 1
b = [a]
"""

    spec = python_to_spec(py)
    assert spec == {
        "type": "let",
        "var": "a",
        "obj": {
            "data": 1
        },
        "sub": {
            "type": "python",
            "name": "b",
            "mod": "tx.parallex.data",
            "func": "_list",
            "params": {
                0: {
                    "name": "a"
                }
            }
        }
    }

def test_python_to_spec_dynamic_list_nested():
    py = """
a = 1
b = [[a]]
"""

    spec = python_to_spec(py)
    assert spec == {
        "type": "let",
        "var": "a",
        "obj": {
            "data": 1
        },
        "sub": {
            "type": "top",
            "sub": [{
                "type": "python",
                "name": "_var_1_0",
                "mod": "tx.parallex.data",
                "func": "_list",
                "params": {
                    0: {
                        "name": "a"
                    }
                }
            }, {
                "type": "python",
                "name": "b",
                "mod": "tx.parallex.data",
                "func": "_list",
                "params": {
                    0: {
                        "name": "_var_1_0",
                    }
                }
            }]
        }
    }

def test_python_to_spec_dynamic_tuple():
    py = """
a = 1
b = (a,)
"""

    spec = python_to_spec(py)
    assert spec == {
        "type": "let",
        "var": "a",
        "obj": {
            "data": 1
        },
        "sub": {
            "type": "python",
            "name": "b",
            "mod": "tx.parallex.data",
            "func": "_tuple",
            "params": {
                0: {
                    "name": "a"
                }
            }
        }
    }

def test_python_to_spec_dynamic_tuple_nested():
    py = """
a = 1
b = ((a,),)
"""

    spec = python_to_spec(py)
    assert spec == {
        "type": "let",
        "var": "a",
        "obj": {
            "data": 1
        },
        "sub": {
            "type": "top",
            "sub": [{
                "type": "python",
                "name": "_var_1_0",
                "mod": "tx.parallex.data",
                "func": "_tuple",
                "params": {
                    0: {
                        "name": "a"
                    }
                }
            }, {
                "type": "python",
                "name": "b",
                "mod": "tx.parallex.data",
                "func": "_tuple",
                "params": {
                    0: {
                        "name": "_var_1_0",
                    }
                }
            }]
        }
    }

def test_python_to_spec_dynamic_dict():
    py = """
a = 1
b = {"t":a}
"""

    spec = python_to_spec(py)
    assert spec == {
        "type": "let",
        "var": "a",
        "obj": {
            "data": 1
        },
        "sub": {
            "type": "python",
            "name": "b",
            "mod": "tx.parallex.data",
            "func": "_dict",
            "params": {
                0: {
                    "data": "t"
                },
                1: {
                    "name": "a"
                }
            }
        }
    }

def test_python_to_spec_dynamic_dict_key():
    py = """
t = "t"
b = {t:1}
"""

    spec = python_to_spec(py)
    assert spec == {
        "type": "let",
        "var": "t",
        "obj": {
            "data": "t"
        },
        "sub": {
            "type": "python",
            "name": "b",
            "mod": "tx.parallex.data",
            "func": "_dict",
            "params": {
                0: {
                    "name": "t"
                },
                1: {
                    "data": 1
                }
            }
        }
    }

def test_python_to_spec_dynamic_dict_nested():
    py = """
a = 1
b = {"s":{"t":a}}
"""

    spec = python_to_spec(py)
    assert spec == {
        "type": "let",
        "var": "a",
        "obj": {
            "data": 1
        },
        "sub": {
            "type": "top",
            "sub": [{
                "type": "python",
                "name": "_var_1_values_0",
                "mod": "tx.parallex.data",
                "func": "_dict",
                "params": {
                    0: {
                        "data": "t"
                    },
                    1: {
                        "name": "a"
                    }
                }
            }, {
                "type": "python",
                "name": "b",
                "mod": "tx.parallex.data",
                "func": "_dict",
                "params": {
                    0: {
                        "data": "s"
                    },
                    1: {
                        "name": "_var_1_values_0",
                    }
                }
            }]
        }
    }

def test_python_to_spec_dynamic_dict_nested_key():
    py = """
t = "t"
b = {"s":{t:1}}
"""

    spec = python_to_spec(py)
    assert spec == {
        "type": "let",
        "var": "t",
        "obj": {
            "data": "t"
        },
        "sub": {
            "type": "top",
            "sub": [{
                "type": "python",
                "name": "_var_1_values_0",
                "mod": "tx.parallex.data",
                "func": "_dict",
                "params": {
                    0: {
                        "name": "t"
                    },
                    1: {
                        "data": 1
                    }
                }
            }, {
                "type": "python",
                "name": "b",
                "mod": "tx.parallex.data",
                "func": "_dict",
                "params": {
                    0: {
                        "data": "s"
                    },
                    1: {
                        "name": "_var_1_values_0",
                    }
                }
            }]
        }
    }

def test_python_to_spec_assign_variable():
    py = """
a = 1
b = a
"""

    spec = python_to_spec(py)
    assert spec == {
        "type": "let",
        "var": "a",
        "obj": {
            "data": 1
        },
        "sub": {
            "type": "python",
            "name": "b",
            "mod": "tx.functional.utils",
            "func": "identity",
            "params": {
                0: {
                    "name": "a"
                }
            }
        }
    }

