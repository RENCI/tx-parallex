from multiprocessing import Manager, Process
from queue import Queue, Empty
import logging
from ctypes import c_int
import time
import pytest
from tx.functional.either import Left, Right
from tx.functional.maybe import Just
from tx.parallex.dependentqueue import DependentQueue, Node
from tx.readable_log import getLogger

logger = getLogger(__name__, logging.DEBUG)


def test_dep():
    with Manager() as manager:
        dq = DependentQueue(manager, None)
        dq.init_thread()

        id3 = dq.put(3)
        id2 = dq.put(2, depends_on={id3: {"a"}})
        id1 = dq.put(1, depends_on={id3: {"a"}, id2: {"b"}})
        
        n, r, sr, f1 = dq.get(block=True)
        assert n == 3
        assert r == {}
        dq.complete(f1, {}, Right({"a": 6}))
        n, r, sr, f2 = dq.get(block=True)
        assert n == 2
        assert r == {"a": 6}
        dq.complete(f2, {}, Right({"b": 5}))
        n, r, sr, f = dq.get(block=True)
        assert n == 1
        assert r == {"b": 5, "a": 6}
        dq.complete(f, {}, Right({"c": 4}))
        n, r, sr, f = dq.get(block=True)
        assert n is None

def test_dep_error():
    with Manager() as manager:
        dq = DependentQueue(manager, None)
        dq.init_thread()

        id3 = dq.put(3)
        id2 = dq.put(2, depends_on={id3: {"a"}})
        id1 = dq.put(1, depends_on={id3: {"a"}, id2: {"b"}})
        
        n, r, sr, f1 = dq.get(block=True)
        assert n == 3
        assert r == {}
        dq.complete(f1, {}, Right({"a": Left("a")}))
        n, r, sr, f2 = dq.get(block=True)
        assert n == 2
        assert r == {"a": Left("a")}

def test_dep_error():
    with Manager() as manager:
        dq = DependentQueue(manager, None)
        dq.init_thread()

        id3 = dq.put(3)
        id2 = dq.put(2, depends_on={id3: {"a"}})
        id1 = dq.put(1, depends_on={id3: {"a"}, id2: {"b"}})
        
        n, r, sr, f1 = dq.get(block=True)
        assert n == 3
        assert r == {}
        dq.complete(f1, {}, Left("a"))
        n, r, sr, f2 = dq.get(block=True)
        assert n == 2
        assert r == {"a": Left("a")}

def test_eoq():

    
    with Manager() as manager:
    
        dq = DependentQueue(manager, 2)
        dq.init_thread()

        def dq_get(v):
            logger.debug("before")
            v.value, _, _, _ = dq.get()
            logger.debug("after")

        id3 = dq.put(2)
        
        logger.debug("get next node")
        n, _, _, f1 = dq.get(block=True)
        logger.debug("next node found")

        v = manager.Value(c_int, 1)
        p = Process(target=dq_get, args=(v,))
        logger.debug("start process")
        p.start()
        time.sleep(1)
        logger.debug("process running")
        
        dq.complete(f1, {}, Just({"a":6}))
        logger.debug("queue completed")
        p.join()
        assert v.value == 2

def test_eoq_2():
    with Manager() as manager:

        dq = DependentQueue(manager, 2)
        dq.init_thread()

        id3 = dq.put(3)
        
        n, r, sr, f1 = dq.get(block=True)
        assert n == 3
        assert r == {}
        dq.complete(f1, {}, Just({"a": 6}))

        n, r, sr, f = dq.get(block=True)
        assert n == 2

# def test_eoq_3():
#     
#         dq = DependentQueue(True)

#         n, r, sr, f = dq.get(block=False)
#         assert n == 2

#         n, r, sr, f = dq.get(block=True)
#         assert n == 2



    
