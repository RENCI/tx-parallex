import sys
from multiprocessing import Manager
from queue import Empty
import pathlib
import importlib
import logging
from tempfile import mkstemp
import shelve
import os
import pytest
from tx.parallex import start, start_python
from tx.parallex.task import enqueue, EndOfQueue
from tx.parallex.io import read_from_disk
from tx.parallex.dependentqueue import DependentQueue
from tx.parallex.spec import dict_to_spec
from tx.functional.maybe import Just
from tx.functional.either import Left, Right
from tx.readable_log import getLogger, format_message

logger = getLogger(__name__, logging.INFO)

def test_enqueue():

    with Manager() as manager:
    
        spec = {
            "type":"map",
            "coll": {
                "name": "inputs"
            },
            "var":"y",
            "sub": {
                "type":"top",
                "sub": [{
                    "type": "python",
                    "name": "a",
                    "mod": "tests.test_task",
                    "func": "f",
                    "params": {
                        "x": {
                            "name": "y"
                        }
                    }
                }]
            }
        }
        data = {
            "inputs": [1, 2, 3]
        }
        dq = DependentQueue(manager, EndOfQueue())

        enqueue(dict_to_spec(spec), data, dq, execute_original=True, level=1)

        n, r, sr, f = dq.get()
        assert "x" in n.kwargs
        assert n.kwargs["x"] in data["inputs"]
        assert r == {}
        dq.complete(f, {}, Just({"a":6}))
        n, r, sr, f = dq.get()
        assert "x" in n.kwargs
        assert n.kwargs["x"] in data["inputs"]
        assert r == {}
        dq.complete(f, {}, Just({"a":6}))
        n, r, sr, f = dq.get()
        assert "x" in n.kwargs
        assert n.kwargs["x"] in data["inputs"]
        dq.complete(f, {}, Just({"a":6}))
        n, r, sr, f = dq.get()
        print(n)
        assert isinstance(n, EndOfQueue)


def test_enqueue_dependent():

    with Manager() as manager:
    
        spec = {
            "type":"top",
            "sub": [{
                "type": "python",
                "name": "a",
                "mod": "tests.test_task",
                "func": "f",
                "params": {
                    "x": {
                        "name": "b"
                    }
                }
            }, {
                "type": "python",
                "name": "b",
                "mod": "tests.test_task",
                "func": "f",
                "params": {
                    "x": {
                        "name": "c"
                    }
                }
            }, {
                "type": "python",
                "name": "c",
                "mod": "tests.test_task",
                "func": "f"
            }]
        }
        data = {}
        dq = DependentQueue(manager, EndOfQueue())

        enqueue(dict_to_spec(spec), data, dq, execute_original=True)

        n, r, sr, f1 = dq.get()
        print(n)
        assert r == {}
        dq.complete(f1, {}, Right({"c":1}))
        n, r, sr, f2 = dq.get()
        print(n)
        assert r == {f1:1}
        dq.complete(f2, {}, Right({"b": 2}))
        n, r, sr, f = dq.get()
        print(n)
        assert r == {f2:2}
        dq.complete(f, {}, Right({"a": 3}))
        n, r, sr, f = dq.get()
        print(n)
        assert isinstance(n, EndOfQueue)

        
def test_execute():

    
        spec = {
            "type": "seq",
            "names": [],
            "sub": [{
                "type":"map",
                "coll": {
                    "name": "inputs"
                },
                "var":"y",
                "sub": {
                    "type":"top",
                    "sub": [{
                        "type": "python",
                        "name": "a",
                        "mod": "tests.test_task",
                        "func": "f",
                        "params": {
                            "x": {
                                "name": "y"
                            }
                        }
                    }, {
                        "type": "ret",
                        "obj": {
                            "name": "a"
                        }
                    }]
                }
            }]
        }
        data = {
            "inputs": [1, 2, 3]
        }
        ret = start(3, spec, data, [], True, None, 1)
        assert ret == {f"{i}": Right(i+2) for i in [0,1,2]}


def test_execute_dependent():

    
        spec = {
            "type":"seq",
            "names": ["a"],
            "sub": [{
                "type": "python",
                "name": "a",
                "mod": "tests.test_task",
                "func": "f",
                "params": {
                    "x": {
                        "name": "b"
                    }
                }
            }, {
                "type": "python",
                "name": "b",
                "mod": "tests.test_task",
                "func": "f",
                "params": {
                    "x": {
                        "name": "c"
                    }
                }
            }, {
                "type": "python",
                "name": "c",
                "mod": "tests.test_task",
                "func": "f",
                "params": {
                    "x": {
                        "data": 0
                    }
                }
            }, {
                "type": "ret",
                "obj": {
                    "name": "a"
                }
            }]
        }
        data = {}
        ret = start(3, spec, data, [], True, None, 1)
        assert ret == {"": Right(3)}

        
def test_level_0():

    with Manager() as manager:
    
    
        spec = {
                "type":"map",
                "coll": {
                    "name": "inputs"
                },
                "var":"y",
                "sub": {
                    "type":"top",
                    "sub": [{
                        "type": "python",
                        "name": "a",
                        "mod": "tests.test_task",
                        "func": "f",
                        "params": {
                            "x": {
                                "name": "y"
                            }
                        }
                    }, {
                        "type": "ret",
                        "obj": {
                            "name": "a"
                        }
                    }]
                }
        }
        data = {
            "inputs": [1, 2, 3]
        }
        dq = DependentQueue(manager, EndOfQueue())

        enqueue(dict_to_spec(spec), data, dq, execute_original=True, level=0)

        assert len(dq.node_map.nodes) == 3
        assert dq.node_map.ready_queue.qsize() == 3
        
        
def test_level_1():

    with Manager() as manager:
    
    
        spec = {
                "type":"map",
                "coll": {
                    "name": "inputs"
                },
                "var":"y",
                "sub": {
                    "type":"top",
                    "sub": [{
                        "type": "python",
                        "name": "a",
                        "mod": "tests.test_task",
                        "func": "f",
                        "params": {
                            "x": {
                                "name": "y"
                            }
                        }
                    }, {
                        "type": "ret",
                        "obj": {
                            "name": "a"
                        }
                    }]
                }
        }
        data = {
            "inputs": [1, 2, 3]
        }
        dq = DependentQueue(manager, EndOfQueue())

        enqueue(dict_to_spec(spec), data, dq, execute_original=True, level=1)

        assert len(dq.node_map.nodes) == 6
        assert dq.node_map.ready_queue.qsize() == 3
        

def test_level_start_0():

        spec = {
                "type":"map",
                "coll": {
                    "name": "inputs"
                },
                "var":"y",
                "sub": {
                    "type":"top",
                    "sub": [{
                        "type": "python",
                        "name": "a",
                        "mod": "tests.test_task",
                        "func": "f",
                        "params": {
                            "x": {
                                "name": "y"
                            }
                        }
                    }, {
                        "type": "ret",
                        "obj": {
                            "name": "a"
                        }
                    }]
                }
        }
        data = {
            "inputs": [1, 2, 3]
        }
        
        ret = start(3, spec, data, [], True, None, 0)
        assert ret == {f"{i}": Right(i+2) for i in [0,1,2]}


def test_level_start_1():

        spec = {
                "type":"map",
                "coll": {
                    "name": "inputs"
                },
                "var":"y",
                "sub": {
                    "type":"top",
                    "sub": [{
                        "type": "python",
                        "name": "a",
                        "mod": "tests.test_task",
                        "func": "f",
                        "params": {
                            "x": {
                                "name": "y"
                            }
                        }
                    }, {
                        "type": "ret",
                        "obj": {
                            "name": "a"
                        }
                    }]
                }
        }
        data = {
            "inputs": [1, 2, 3]
        }
        
        ret = start(3, spec, data, [], True, None, 1)
        assert ret == {f"{i}": Right(i+2) for i in [0,1,2]}


def test_dynamic_level_start_0():

    py = """
from tx.functional.utils import identity
from tests.test_task import f
a = identity(inputs)
for i in a:
    b = f(i)
    return b
"""

    data = {
        "inputs": [1, 2, 3]
    }
        
    ret = start_python(3, py, data, [], True, None, 0)
    assert ret == {f"{i}": Right(i+2) for i in [0,1,2]}


def test_dynamci_level_start_1():

    py = """
from tx.functional.utils import identity
from tests.test_task import f
a = identity(inputs)
for i in a:
    b = f(i)
    return b
"""

    data = {
        "inputs": [1, 2, 3]
    }
        
    ret = start_python(3, py, data, [], True, None, 1)
    assert ret == {f"{i}": Right(i+2) for i in [0,1,2]}


def test_let():

    
        spec = {
            "type":"let",
            "var": "y",
            "obj": {
                "data": 1
            },
            "sub": {
                "type": "top",
                "sub": [{
                    "type": "python",
                    "name": "a",
                    "mod": "tx.functional.utils",
                    "func": "identity",
                    "params": {
                        0: {
                            "name": "y"
                        }
                    }
                }, {
                    "type": "ret",    
                    "obj": {
                        "name": "a"
                    }
                }]
            }
        }
        data = {}
        ret = start(3, spec, data, [], True, None, 1)
        assert ret == {"": Right(1)}

        
def f(x):
    logger.debug(format_message("f", "start", {"x": x}))
    return x+1

def g(x,y):
    return x+y

def test_start():

    
        spec = {
            "type":"top",
            "sub": [{
                "type": "python",
                "name": "a",
                "mod": "tests.test_task",
                "func": "f",
                "params": {
                    "x": {
                        "name": "b"
                    }
                }
            }, {
                "type": "python",
                "name": "b",
                "mod": "tests.test_task",
                "func": "f",
                "params": {
                    "x": {
                        "name": "c"
                    }
                }
            }, {
                "type": "python",
                "name": "c",
                "mod": "tests.test_task",
                "func": "f",
                "params": {
                    "x": {
                        "name": "y"
                    }
                }
            }, {
                "type": "ret",
                "obj": {
                    "name": "a"
                }
            }]
        }
        data = {"y": 1}
        
        ret = start(3, spec, data, [], True, None, 1)
        assert ret == {"": Right(4)}


def test_map_start():

    
        spec = {
            "type": "map",
            "coll": {
                "name": "z"
            },
            "var": "y",
            "sub": {
                "type":"top",
                "sub": [{
                    "type": "python",
                    "name": "a",
                    "mod": "tests.test_task",
                    "func": "f",
                    "params": {
                        "x": {
                            "name": "b"
                        }
                    }
                }, {
                    "type": "python",
                    "name": "b",
                    "mod": "tests.test_task",
                    "func": "f",
                    "params": {
                        "x": {
                            "name": "c"
                        }
                    }
                }, {
                    "type": "python",
                    "name": "c",
                    "mod": "tests.test_task",
                    "func": "f",
                    "params": {
                        "x": {
                            "name": "y"
                        }
                    }
                }, {
                    "type": "ret",
                    "obj": {
                        "name": "a"
                    }
                }]
            }
        }
        data = {"z": [1, 2, 3]}
        
        ret = start(3, spec, data, [], True, None, 1)
        assert ret == {"0": Right(4), "1": Right(5), "2": Right(6)}


def test_cond_then_start():
    
    
        spec = {
            "type": "cond",
            "on": {
                "name": "z"
            },
            "then": {
                "type": "ret",
                "obj": {
                    "data": 1
                }
            },
            "else": {
                "type": "ret",
                "obj": {
                    "data": 0
                }
            }
        }
        data = {"z": True}
        
        ret = start(3, spec, data, [], True, None, 1)
        assert ret == {"": Right(1)}


def test_cond_else_start():
    
    
        spec = {
            "type": "cond",
            "on": {
                "name": "z"
            },
            "then": {
                "type": "ret",
                "obj": {
                    "data": 1
                }
            },
            "else": {
                "type": "ret",
                "obj": {
                    "data": 0
                }
            }
        }
        data = {"z": False}
        
        ret = start(3, spec, data, [], True, None, 1)
        assert ret == {"": Right(0)}


def false():
    return False

def true():
    return True

def test_dynamic_cond_then_start():
    
    
        spec = {
            "type": "top",
            "sub":[{
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
                    "obj": {
                        "data": 1
                    }
                },
                "else": {
                    "type": "ret",
                    "obj": {
                        "data": 0
                    }
                }
            }]
        }
        data = {"z": True}
        
        ret = start(3, spec, data, [], True, None, 1)
        assert ret == {"": Right(1)}


def test_dynamic_cond_else_start():
    
    
        spec = {
            "type": "top",
            "sub":[{
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
                    "obj": {
                        "data": 1
                    }
                },
                "else": {
                    "type": "ret",
                    "obj": {
                        "data": 0
                    }
                }
            }]
        }
        data = {"z": False}
        
        ret = start(3, spec, data, [], True, None, 1)
        assert ret == {"": Right(0)}


def test_dsl_start():
    
    
        py = """
a = tests.test_task.f(x=b)
b = tests.test_task.f(x=c)
c = tests.test_task.f(x=y)
return a"""

        data = {"y": 1}
        
        ret = start_python(3, py, data, [], True, None, 1)
        assert ret == {"": Right(4)}


def test_dsl_depend_for_to_outer_start():
    
    
        py = """
y = 1
d = [2,3]
c = tests.test_task.f(x=y)
for j in d:
    a = tests.test_task.g(x=c,y=j)
    return a"""

        data = {}
        
        ret = start_python(3, py, data, [], True, None, 1)
        assert ret == {"0": Right(4), "1": Right(5)}


def test_return():
    
        py = """
return True
"""
        data = {}

        ret = start_python(3, py, data, [], True, None, 1)
        assert ret == {"": Right(True)}


def test_dynamic_for_0():
    
    
        py = """
d = [2,3]
c = tx.functional.utils.identity(d)
for j in c:
    a = tests.test_task.g(x=2,y=j)
    return a"""

        data = {}
        
        ret = start_python(3, py, data, [], True, None, 1)
        assert ret == {"0": Right(4), "1": Right(5)}


def test_dynamic_return():
    
    
        py = """
c = tx.functional.utils.identity([0])
return c"""

        data = {}
        
        ret = start_python(3, py, data, [], True, None, 1)
        assert ret == {"": Right([0])}


def test_dynamic_for_1():
    
    
        py = """
c = tx.functional.utils.identity([0])
for j in c:
    return j"""

        data = {}
        
        ret = start_python(3, py, data, [], True, None, 1)
        assert ret == {"0": Right(0)}


def test_dynamic_for_2():
    
    
        py = """
from tx.functional.utils import identity
d = identity(2)
c = identity([2,3])
for j in c:
    return d+j"""

        data = {}
        
        ret = start_python(3, py, data, [], True, None, 1)
        assert ret == {"0": Right(4), "1": Right(5)}


def test_dynamic_type_error():
    
    
        py = """
from tx.functional.utils import identity
d = identity(2)
c = identity([2,3])
return d+c"""
        
        data = {}
        
        ret = start_python(3, py, data, [], True, None, 1)
        logger.debug(format_message("type_dynamic_type_error", "dynamic type error", {"ret": ret}))
        assert isinstance(ret[""], Left)


def test_dynamic_type_error_2():
    
    
        py = """
c = tx.functional.utils.identity([2])
for j in c:
    return 2+c"""

        data = {}
        
        ret = start_python(3, py, data, [], True, None, 1)
        assert isinstance(ret["0"], Left)


def test_circular_dependency():
    
    with pytest.raises(RuntimeError) as excinfo:
        
        py = """
c = tx.functional.utils.identity(d)
d = tx.functional.utils.identity(c)"""

        data = {}
        
        ret = start_python(3, py, data, [], True, None, 1)
        assert str(excinfo.value) == "RuntimeError: unresolved dependencies or cycle in depedencies graph visited = set()"


def test_dynamic_if():
    
    
        py = """
from tx.functional.utils import identity
if identity(True):
        return 1
else:
        return 0"""

        data = {}
        
        ret = start_python(3, py, data, [], True, None, 1)
        assert ret == {"": Right(1)}

        
def test_dynamic_if_2():
    
    
        py = """
from tx.functional.utils import identity
if identity(False):
        return 1
else:
        return 0"""

        data = {}
        
        ret = start_python(3, py, data, [], True, None, 1)
        assert ret == {"": Right(0)}

        
def test_dynamic_if_3():
    
    
        py = """
from tx.functional.utils import identity
i = identity(True)
if i:
        return i
else:
        return i"""

        data = {}
        
        ret = start_python(3, py, data, [], True, None, 1)
        assert ret == {"": Right(True)}

        
def test_dynamic_if_4():
    
    
        py = """
from tx.functional.utils import identity
i = identity(False)
if i:
        return i
else:
        return i"""

        data = {}
        
        ret = start_python(3, py, data, [], True, None, 1)
        assert ret == {"": Right(False)}

        
def test_dynamic_if_5():
    
    
        py = """
from tx.functional.utils import identity
if identity(True):
        x=identity(1)
        return x"""

        data = {}
        
        ret = start_python(3, py, data, [], True, None, 1)
        assert ret == {"": Right(1)}


def test_dynamic_for_10():
    for i in range(20):
        logger.info(f"test start {i} ***************************************")
        
        py = """
for j in tx.functional.utils.identity([1]):
    return j + 2"""

        data = {}
        
        ret = start_python(3, py, data, [], True, None, 1)
        assert ret == {"0": Right(3)}


def test_data_start():
    
    
        py = """
a = tests.test_task.f(x=1)
return a"""

        data = {}
        
        ret = start_python(3, py, data, [], True, None, 1)
        assert ret == {"": Right(2)}


def test_args_start():
    
    
        py = """
a = tests.test_task.f(1)
return a"""

        data = {}
        
        ret = start_python(3, py, data, [], True, None, 1)
        assert ret == {"": Right(2)}


def test_dep_args_start():
    
    
        py = """
a = tests.test_task.f(1)
b = tests.test_task.f(a)
return b"""

        data = {}
        
        ret = start_python(3, py, data, [], True, None, 1)
        assert ret == {"": Right(3)}


def add(a,b):
    return a+b


def test_map_data_start():
    
        py = """
for s in [1,2,3,4,5,6,7]:
    t = tests.test_task.add(a=s, b=1)
    return t"""

        data = {}
        
        ret = start_python(3, py, data, [], True, None, 1)
        assert ret == {f"{i}": Right(i+2) for i in range(0,7)}

        
def test_shared_object_reference_counting():
    
    
        py = """
s = tx.functional.utils.identity(0.5)
t = tests.test_task.add(a=s, b=s)
return t"""

        data = {}
        
        ret = start_python(3, py, data, [], True, None, 1)
        assert ret == {"": Right(1)}

        
def test_system_function():
    
    
        py = """
a = all([True])
return a"""

        data = {}
        
        ret = start_python(3, py, data, [], True, None, 1)
        assert ret == {"": Right(True)}

        
def test_if_exp():
    
    
        py = """
a = 1 if True else 0
return a"""

        data = {}
        
        ret = start_python(3, py, data, [], True, None, 1)
        assert ret == {"": Right(1)}

        
def test_var_in_dict_lit():
    
    
        py = """
a = tx.functional.utils.identity(1)
b = {"t": a}
return b"""

        data = {}
        
        ret = start_python(3, py, data, [], True, None, 1)
        assert ret == {"": Right({"t": 1})}

        
def test_var_in_dict_lit_2():
    
    
        py = """
a = tx.functional.utils.identity({"t": 1})
b = {"s":0, **a}
return b"""

        data = {}
        
        ret = start_python(3, py, data, [], True, None, 1)
        assert ret == {"": Right({"s": 0, "t": 1})}

        
def test_var_in_list_lit():
    
    
        py = """
a = tx.functional.utils.identity(1)
b = [a]
return b"""

        data = {}
        
        ret = start_python(3, py, data, [], True, None, 1)
        assert ret == {"": Right([1])}

        
def test_var_in_list_lit():
    
    
        py = """
a = tx.functional.utils.identity(1)
b = (a,)
return b"""

        data = {}
        
        ret = start_python(3, py, data, [], True, None, 1)
        assert ret == {"": Right((1,))}

        
def test_subscript():
    
    
        py = """
a = [0,1,2][1]
return a"""

        data = {}
        
        ret = start_python(3, py, data, [], True, None, 1)
        assert ret == {"": Right(1)}


def test_starred_var_in_list_lit():
    
        py = """
b = [1]
return [*b]
"""
        data = {}
        ret = start_python(3, py, data, [], True, None, 1)
        assert ret == {"": Right([1])}
        
def test_starred_var_in_dict_lit():
    
        py = """
b = {"t":1}
return {**b}
"""
        data = {}
        ret = start_python(3, py, data, [], True, None, 1)
        assert ret == {"": Right({"t":1})}
        
def test_destructure():
    
        py = """
a,b,c = [1,2,3]
return a+b+c
"""
        data = {}
        ret = start_python(3, py, data, [], True, None, 1)
        assert ret == {"": Right(6)}
        
def runtime_error():
    raise RuntimeError()


def return_error():
    return Left("errmsg")


def test_exception_error():
    
        py = """
t = tests.test_task.runtime_error()
return t
"""
        data = {}

        ret = start_python(3, py, data, [], True, None, 1)
        assert isinstance(ret[""], Left)

        
def test_return_error():
    
        py = """
t = tests.test_task.return_error()
return t
"""
        data = {}

        ret = start_python(3, py, data, [], True, None, 1)
        assert isinstance(ret[""], Left)


def test_args_order():
    
        py = """
t = tx.functional.utils.identity(2)
return t - 1
"""
        data = {}

        ret = start_python(3, py, data, [], True, None, 1)
        assert ret == {"": Right(1)}


def test_system_paths():
    
        py = """
t = mod.func(1)
return t
"""
        data = {}

        ret = start_python(3, py, data, [str(pathlib.Path(__file__).parent.absolute() / "user")], True, None, 1)
        assert ret == {"": Right(1)}

    
def test_system_paths_2():
    
        py = """
from mod import func
return func(1)
"""
        data = {}

        ret = start_python(3, py, data, [str(pathlib.Path(__file__).parent.absolute() / "user")], True, None, 1)
        assert ret == {"": Right(1)}

    
def test_system_paths_3():
    
        py = """
from mod import *
return func(1)
"""
        data = {}

        ret = start_python(3, py, data, [str(pathlib.Path(__file__).parent.absolute() / "user")], True, None, 1)
        assert ret == {"": Right(1)}


def test_system_paths_4():
    
        py = """
return 1
"""
        data = {}

        ret = start_python(3, py, data, [str(pathlib.Path(__file__).parent.absolute() / "user")], True, None, 1)
        with pytest.raises(Exception) as excinfo:
            importlib.import_module("cd")


def test_system_paths_5():
    
        p = str(pathlib.Path(__file__).parent.absolute() / "user")
        sys.path.append(p)
        py = """
return 1
"""
        data = {}

        ret = start_python(3, py, data, [p], True, None, 1)
        importlib.import_module("mod")
        sys.path.remove(p)


def test_output_path():
    _, temp_path = mkstemp()
    os.remove(temp_path)
    try:
        
            py = """
return 1
"""
            data = {}

            start_python(3, py, data, [], True, temp_path, 1)
            assert read_from_disk(temp_path) == {"": Right(1)}
            assert(os.path.isfile(f"{temp_path}"))
    finally:
        os.remove(f"{temp_path}")

        
def test_output_path_2():
    temp_path = "/tmp/out"
    try:
        
            py = """
return 1
"""
            data = {}

            start_python(3, py, data, [], True, temp_path, 1)
            assert read_from_disk(temp_path) == {"": Right(1)}
    finally:
        os.remove(f"{temp_path}")


def test_output_path_3():
    temp_path = "/tmp/out"
    try:
        
            py = """
return 0
"""
            py2 = """
return 1
"""
            data = {}

            start_python(3, py, data, [], True, temp_path, 1)
            assert read_from_disk(temp_path) == {"": Right(0)}
                
            start_python(3, py2, data, [], True, temp_path, 1)
            assert read_from_disk(temp_path) == {"": Right(1)}
    finally:
        os.remove(f"{temp_path}")


