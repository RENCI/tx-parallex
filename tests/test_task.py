from multiprocessing import Manager
from queue import Empty
import pytest
from tx.parallex import start, start_python
from tx.parallex.task import enqueue, EndOfQueue
from tx.parallex.dependentqueue import DependentQueue
from tx.functional.either import Left, Right

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
        dq = DependentQueue(manager)

        enqueue(spec, data, dq)

        n, r, f = dq.get(block=False)
        assert n.kwargs == {"x":1}
        assert r == {}
        dq.complete(f, 6)
        n, r, f = dq.get(block=False)
        assert n.kwargs == {"x":2}
        assert r == {}
        dq.complete(f, 6)
        n, r, f = dq.get(block=False)
        assert n.kwargs == {"x":3}
        dq.complete(f, 6)
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
        dq = DependentQueue(manager)

        enqueue(spec, data, dq)

        n, r, f = dq.get(block=False)
        print(n)
        assert r == {}
        dq.complete(f, 1)
        n, r, f = dq.get(block=False)
        print(n)
        assert r == {"x":1}
        dq.complete(f, 2)
        n, r, f = dq.get(block=False)
        print(n)
        assert r == {"x":2}
        dq.complete(f, 3)
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
            "obj": {
                "y": 1
            },
            "sub": {
                "type": "python",
                "name": "a",
                "mod": "tests.test_task",
                "func": "identity",
                "params": {
                    "x": {
                        "name": "y"
                    }
                },
                "ret": ["x"]
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
                "ret": ["x"],
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
                    "ret": ["x"],
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
                }]
            }
        }
        data = {"z": [1, 2, 3]}
        
        ret = start(3, spec, data)
        assert ret == {"0.x": Right(4), "1.x": Right(5), "2.x": Right(6)}


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
