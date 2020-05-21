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
        spec = {
            "type":"dsl",
            "python": """
a = tests.test_task.f(x=b)
b = tests.test_task.f(x=c)
c = tests.test_task.f(x=y)
return {"x": a}"""
        }
        data = {"y": 1}
        
        ret = start(3, spec, data)
        assert ret == {"x": Right(4)}


def test_data_start():
    print("test_start")
    with Manager() as manager:
        spec = {
            "type":"dsl",
            "python": """
a = tests.test_task.f(x=1)
return {"x": a}"""
        }
        data = {}
        
        ret = start(3, spec, data)
        assert ret == {"x": Right(2)}


def test_args_start():
    print("test_start")
    with Manager() as manager:
        spec = {
            "type":"dsl",
            "python": """
a = tests.test_task.f(1)
return {"x": a}"""
        }
        data = {}
        
        ret = start(3, spec, data)
        assert ret == {"x": Right(2)}


def add(a,b):
    return a+b


def test_map_data_start():
    print("test_start")
    with Manager() as manager:
        spec = {
            "type":"map",
            "var": "s",
            "coll": {
                "data": [1,2,3,4,5,6,7]
            },
            "sub": {
                "type": "dsl",
                "python": """
t = tests.test_task.add(a=s, b=1)
return {"t": t}"""
            }
        }
        data = {}
        
        ret = start(3, spec, data)
        assert ret == {f"{i}.t": Right(i+2) for i in range(0,7)}


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
                "param": {
                    "name": "arg"
                }
            },
            "ret": []
        }]
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
            "params": {},
            "ret": []
        }, {
            "type": "python",
            "name": "a",
            "mod": "mod1.mod2",
            "func": "func",
            "params": {
                "param": {
                    "depends_on": "var"
                }
            },
            "ret": []
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
            "params": {},
            "ret": []
        }, {
            "type": "python",
            "name": "a",
            "mod": "mod1.mod2",
            "func": "func",
            "params": {
                "param": {
                    "depends_on": "var"
                }
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
            "type": "top",
            "sub": [{
                "type": "python",
                "name": "x",
                "mod": "mod1",
                "func": "func2",
                "params": {
                    "r": {
                        "name": "i"
                    }
                },
                "ret": []
            }]
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
        "obj": {
            "y": 1
        },
        "sub": {
            "type": "map",
            "var": "i",
            "coll": {
                "name": "c"
            },
            "sub": {
                "type": "top",
                "sub": [{
                    "type": "python",
                    "name": "x",
                    "mod": "mod1",
                    "func": "func2",
                    "params": {
                        "r": {
                            "name": "i"
                        }
                    },
                    "ret": []
                }]
            }
        }
    }

def test_python_to_spec7():
    py = """x = mod1.func2(i)"""

    spec = python_to_spec(py)
    assert spec == {
        "type": "top",
        "sub": [{
            "type": "python",
            "name": "x",
            "mod": "mod1",
            "func": "func2",
            "params": {
                0: {
                    "name": "i"
                }
            },
            "ret": []
        }]
    }

