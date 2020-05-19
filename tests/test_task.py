from multiprocessing import Manager
from queue import Empty
import pytest
from tx.parallex import start
from tx.parallex.task import enqueue, EndOfQueue, python_to_spec
from tx.parallex.dependentqueue import DependentQueue
from tx.functional.either import Left, Right

def test_enqueue():
    print("test_enqueue")
    with Manager() as manager:
        spec = {
            "type":"map",
            "coll":"inputs",
            "var":"y",
            "sub": {
                "type":"top",
                "sub": [{
                    "type": "python",
                    "name": "a",
                    "mod": "tests.test_task",
                    "func": "f",
                    "params": {
                        "y": ["x"]
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
                "depends_on": {"b": ["x"]}
            }, {
                "type": "python",
                "name": "b",
                "mod": "tests.test_task",
                "func": "f",
                "depends_on": {"c": ["x"]}
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
                    "y": ["x"]
                },
                "ret": ["x"]
            }
        }
        data = {}
        ret = start(3, spec, data)
        assert ret == {"x": Right(1)}

        
def f(x):
    return x+1


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
                "depends_on": {"b": ["x"]}
            }, {
                "type": "python",
                "name": "b",
                "mod": "tests.test_task",
                "func": "f",
                "depends_on": {"c": ["x"]}
            }, {
                "type": "python",
                "name": "c",
                "mod": "tests.test_task",
                "func": "f",
                "params": {
                    "y": ["x"]
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
            "coll": "z",
            "var": "y",
            "sub": {
                "type":"top",
                "sub": [{
                    "type": "python",
                    "name": "a",
                    "mod": "tests.test_task",
                    "func": "f",
                    "ret": ["x"],
                    "depends_on": {"b": ["x"]}
                }, {
                    "type": "python",
                    "name": "b",
                    "mod": "tests.test_task",
                    "func": "f",
                    "depends_on": {"c": ["x"]}
                }, {
                    "type": "python",
                    "name": "c",
                    "mod": "tests.test_task",
                    "func": "f",
                    "params": {
                        "y": ["x"]
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
        spec = {
            "type":"dsl",
            "python": """
a = tests.test_task.f(x=~b)
b = tests.test_task.f(x=~c)
c = tests.test_task.f(x=y)
return {"x": a}"""
        }
        data = {"y": 1}
        
        ret = start(3, spec, data)
        assert ret == {"x": Right(4)}


def test_python_to_spec1():
    py = "a = mod1.mod2.func(param=arg)"
    spec = python_to_spec(py)
    assert spec == {
        "type":"top",
        "sub": [{
            "type": "python",
            "name": "a",
            "mod": "mod1.mod2",
            "func": "func",
            "params": {
                "arg": [
                    "param"
                ]
            },
            "depends_on": {
            },
            "ret": []
        }]
    }


def test_python_to_spec2():
    py = "a = mod1.mod2.func(param=~var)"
    spec = python_to_spec(py)
    assert spec == {
        "type":"top",
        "sub": [{
            "type": "python",
            "name": "a",
            "mod": "mod1.mod2",
            "func": "func",
            "params": {
            },
            "depends_on": {
                "var": [
                    "param"
                ]
            },
            "ret": []
        }]       
    }

def test_python_to_spec3():
    py = """
a = mod1.mod2.func(param=~var)
return {'ret1': a}"""
    spec = python_to_spec(py)
    assert spec == {
        "type":"top",
        "sub": [{
            "type": "python",
            "name": "a",
            "mod": "mod1.mod2",
            "func": "func",
            "params": {
            },
            "depends_on": {
                "var": [
                    "param"
                ]
            },
            "ret": ["ret1"]
        }]
    }

def test_python_to_spec4():
    py = "a = 1"
    spec = python_to_spec(py)
    assert spec == {
        "type":"let",
        "obj": {
            "a": 1
        },
        "sub": {
            "type": "top",
            "sub": []
        }
    }


