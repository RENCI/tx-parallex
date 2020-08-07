from multiprocessing import Manager, Process
from queue import Queue, Empty
import logging
from ctypes import c_int
import time
import pytest
from tx.functional.maybe import Just
from tx.parallex.dependentqueue import DependentQueue, Node
from tx.readable_log import getLogger

logger = getLogger(__name__, logging.DEBUG)


def test_dep():
    with Manager() as manager:
        dq = DependentQueue(manager, None)

        id3 = dq.put(3)
        id2 = dq.put(2, depends_on={id3})
        id1 = dq.put(1, depends_on={id3, id2})
        
        n, r, sr, f1 = dq.get(block=True)
        assert n == 3
        assert r == {}
        dq.complete(f1, {}, Just(6))
        n, r, sr, f2 = dq.get(block=True)
        assert n == 2
        assert r == {f1: 6}
        dq.complete(f2, {}, Just(5))
        n, r, sr, f = dq.get(block=True)
        assert n == 1
        assert r == {f2: 5, f1: 6}
        dq.complete(f, {}, Just(4))
        n, r, sr, f = dq.get(block=True)
        assert n is None

def test_eoq():

    
    with Manager() as manager:
        dq = DependentQueue(manager, 2)

        def dq_get(v):
            logger.debug("before")
            v.value = dq.get()
            logger.debug("after")

        id3 = dq.put(2)
        
        logger.debug("get next node")
        n, _, _, f1 = dq.get(block=True)
        logger.debug("next node found")

        v = manager.Value(c_int, 1)
        p = Process(target=dq_get, args=[v])
        logger.debug("start process")
        p.start()
        time.sleep(1)
        logger.debug("process running")
        
        dq.complete(f1, {}, Just(6))
        logger.debug("queue completed")
        p.join()
        assert v.value[0] == 2

def test_eoq_2():
    with Manager() as manager:
        dq = DependentQueue(manager, 2)

        id3 = dq.put(3)
        
        n, r, sr, f1 = dq.get(block=True)
        assert n == 3
        assert r == {}
        dq.complete(f1, {}, Just(6))

        n, r, sr, f = dq.get(block=True)
        assert n == 2

# def test_eoq_3():
#     with Manager() as manager:
#         dq = DependentQueue(manager, True)

#         n, r, sr, f = dq.get(block=False)
#         assert n == 2

#         n, r, sr, f = dq.get(block=True)
#         assert n == 2



    
