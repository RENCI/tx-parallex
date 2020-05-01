from multiprocessing import Manager
from queue import Empty
import pytest
from parallex.task import enqueue, EndOfQueue, start
from parallex.dependentqueue import DependentQueue

def test_enqueue():
    with Manager() as manager:
        spec = {
            "type":"map",
            "coll":"inputs",
            "var":"x",
            "sub": [{
                "type":"top",
                "sub": [{
                    "type": "python",
                    "name": "a",
                    "mod": "tests.test_task",
                    "func": "f",
                    "params": ["x"]
                }]
            }]
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

def f(x):
    return x+1

def test_start():
    with Manager() as manager:
        spec = {
            "type":"top",
            "sub": [{
                "type": "python",
                "name": "a",
                "mod": "tests.test_task",
                "func": "f",
                "ret": "x",
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
                "params": ["x"]
            }]
        }
        data = {"x": 1}
        
        ret = start(3, spec, data)
        assert ret == {"x": 4}


