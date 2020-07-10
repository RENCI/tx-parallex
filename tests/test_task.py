from multiprocessing import Manager
from queue import Empty
import logging
import pytest
from tx.parallex import start, start_python
from tx.parallex.task import enqueue, EndOfQueue
from tx.parallex.dependentqueue import DependentQueue
from tx.functional.either import Left, Right
from tx.readable_log import getLogger

logger = getLogger(__name__, logging.INFO)

def test_enqueue():
    print("test_enqueue")
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

        enqueue(spec, data, dq, execute_unreachable=True)

        n, r, f = dq.get(block=False)
        assert n.kwargs == {"x":1}
        assert r == {}
        dq.complete(f, {}, 6)
        n, r, f = dq.get(block=False)
        assert n.kwargs == {"x":2}
        assert r == {}
        dq.complete(f, {}, 6)
        n, r, f = dq.get(block=False)
        assert n.kwargs == {"x":3}
        dq.complete(f, {}, 6)
        n, r, f = dq.get(block=False)
        print(n)
        assert isinstance(n, EndOfQueue)


def test_enqueue_dependent():
    print("test_enqueue_dependent")
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
                        "depends_on": "b"
                    }
                }
            }, {
                "type": "python",
                "name": "b",
                "mod": "tests.test_task",
                "func": "f",
                "params": {
                    "x": {
                        "depends_on": "c"
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

        enqueue(spec, data, dq, execute_unreachable=True)

        n, r, f1 = dq.get(block=False)
        print(n)
        assert r == {}
        dq.complete(f1, {}, 1)
        n, r, f2 = dq.get(block=False)
        print(n)
        assert r == {f1:1}
        dq.complete(f2, {}, 2)
        n, r, f = dq.get(block=False)
        print(n)
        assert r == {f2:2}
        dq.complete(f, {}, 3)
        n, r, f = dq.get(block=False)
        print(n)
        assert isinstance(n, EndOfQueue)

        
def identity(x):
    return x


def test_let():
    print("test_let")
    with Manager() as manager:
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
                    "mod": "tests.test_task",
                    "func": "identity",
                    "params": {
                        "x": {
                            "name": "y"
                        }
                    }
                }, {
                    "type": "ret",    
                    "var": "x",
                    "obj": {
                        "depends_on": "a"
                    }
                }]
            }
        }
        data = {}
        ret = start(3, spec, data)
        assert ret == {"x": Right(1)}

        
def f(x):
    return x+1

def g(x,y):
    return x+y

def test_start():
    print("test_start")
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
                        "depends_on": "b"
                    }
                }
            }, {
                "type": "python",
                "name": "b",
                "mod": "tests.test_task",
                "func": "f",
                "params": {
                    "x": {
                        "depends_on": "c"
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
                "var": "x",
                "obj": {
                    "depends_on": "a"
                }
            }]
        }
        data = {"y": 1}
        
        ret = start(3, spec, data)
        assert ret == {"x": Right(4)}


def test_map_start():
    print("test_start")
    with Manager() as manager:
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
                            "depends_on": "b"
                        }
                    }
                }, {
                    "type": "python",
                    "name": "b",
                    "mod": "tests.test_task",
                    "func": "f",
                    "params": {
                        "x": {
                            "depends_on": "c"
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
                    "var": "x",
                    "obj": {
                        "depends_on": "a"
                    }
                }]
            }
        }
        data = {"z": [1, 2, 3]}
        
        ret = start(3, spec, data)
        assert ret == {"0.x": Right(4), "1.x": Right(5), "2.x": Right(6)}


def test_cond_then_start():
    print("test_start")
    with Manager() as manager:
        spec = {
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
        data = {"z": True}
        
        ret = start(3, spec, data)
        assert ret == {"x": Right(1)}


def test_cond_else_start():
    print("test_start")
    with Manager() as manager:
        spec = {
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
        data = {"z": False}
        
        ret = start(3, spec, data)
        assert ret == {"x": Right(0)}


def false():
    return False

def true():
    return True

def test_dynamic_cond_then_start():
    print("test_start")
    with Manager() as manager:
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
                    "depends_on": "z"
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
        data = {"z": True}
        
        ret = start(3, spec, data)
        assert ret == {"x": Right(1)}


def test_dynamic_cond_else_start():
    print("test_start")
    with Manager() as manager:
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
                    "depends_on": "z"
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
        data = {"z": False}
        
        ret = start(3, spec, data)
        assert ret == {"x": Right(0)}


def test_dsl_start():
    print("test_start")
    with Manager() as manager:
        py = """
a = tests.test_task.f(x=b)
b = tests.test_task.f(x=c)
c = tests.test_task.f(x=y)
return {"x": a}"""

        data = {"y": 1}
        
        ret = start_python(3, py, data)
        assert ret == {"x": Right(4)}


def test_dsl_depend_for_to_outer_start():
    print("test_start")
    with Manager() as manager:
        py = """
y = 1
d = [2,3]
c = tests.test_task.f(x=y)
for j in d:
    a = tests.test_task.g(x=c,y=j)
    return {"x": a}"""

        data = {}
        
        ret = start_python(3, py, data)
        assert ret == {"0.x": Right(4), "1.x": Right(5)}


def test_dynamic_for():
    print("test_start")
    with Manager() as manager:
        py = """
d = [2,3]
c = tests.test_task.identity(d)
for j in c:
    a = tests.test_task.g(x=2,y=j)
    return {"x": a}"""

        data = {}
        
        ret = start_python(3, py, data)
        assert ret == {"0.x": Right(4), "1.x": Right(5)}


def test_dynamic_for_10():
    for i in range(20):
        logger.info(f"test start {i} ***************************************")
        with Manager() as manager:
            py = """
for j in tests.test_task.identity([1]):
    return {"x": j + 2}"""

            data = {}
        
            ret = start_python(3, py, data)
            assert ret == {"0.x": Right(3)}


def test_data_start():
    print("test_start")
    with Manager() as manager:
        py = """
a = tests.test_task.f(x=1)
return {"x": a}"""

        data = {}
        
        ret = start_python(3, py, data)
        assert ret == {"x": Right(2)}


def test_args_start():
    print("test_start")
    with Manager() as manager:
        py = """
a = tests.test_task.f(1)
return {"x": a}"""

        data = {}
        
        ret = start_python(3, py, data)
        assert ret == {"x": Right(2)}


def test_dep_args_start():
    print("test_start")
    with Manager() as manager:
        py = """
a = tests.test_task.f(1)
b = tests.test_task.f(a)
return {"x": b}"""

        data = {}
        
        ret = start_python(3, py, data)
        assert ret == {"x": Right(3)}


def add(a,b):
    return a+b


def test_map_data_start():
    print("test_start")
    with Manager() as manager:
        py = """
for s in [1,2,3,4,5,6,7]:
    t = tests.test_task.add(a=s, b=1)
    return {"t": t}"""

        data = {}
        
        ret = start_python(3, py, data)
        assert ret == {f"{i}.t": Right(i+2) for i in range(0,7)}

        
def test_system_function():
    print("test_start")
    with Manager() as manager:
        py = """
a = all([True])
return {"t": a}"""

        data = {}
        
        ret = start_python(3, py, data)
        assert ret == {f"t": Right(True)}

        
def test_if_exp():
    print("test_start")
    with Manager() as manager:
        py = """
a = 1 if True else 0
return {"t": a}"""

        data = {}
        
        ret = start_python(3, py, data)
        assert ret == {f"t": Right(1)}

        
def runtime_error():
    raise RuntimeError()


def return_error():
    return Left("errmsg")


def test_exception_error():
    with Manager() as manager:
        py = """
t = tests.test_task.runtime_error()
return {"t": t}
"""
        data = {}

        ret = start_python(3, py, data)
        assert isinstance(ret["t"], Left)

        
def test_return_error():
    with Manager() as manager:
        py = """
t = tests.test_task.return_error()
return {"t": t}
"""
        data = {}

        ret = start_python(3, py, data)
        assert isinstance(ret["t"], Left)
