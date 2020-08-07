from threading import Thread
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
    
        dq = DependentQueue(None)

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

    
    
        dq = DependentQueue(2)

        def dq_get():
            logger.debug("before")
            nonlocal v
            v = dq.get()
            logger.debug("after")

        id3 = dq.put(2)
        
        logger.debug("get next node")
        n, _, _, f1 = dq.get(block=True)
        logger.debug("next node found")

        v = 1
        p = Thread(target=dq_get)
        logger.debug("start process")
        p.start()
        time.sleep(1)
        logger.debug("process running")
        
        dq.complete(f1, {}, Just(6))
        logger.debug("queue completed")
        p.join()
        assert v[0] == 2

def test_eoq_2():

        dq = DependentQueue(2)

        id3 = dq.put(3)
        
        n, r, sr, f1 = dq.get(block=True)
        assert n == 3
        assert r == {}
        dq.complete(f1, {}, Just(6))

        n, r, sr, f = dq.get(block=True)
        assert n == 2

# def test_eoq_3():
#     
#         dq = DependentQueue(True)

#         n, r, sr, f = dq.get(block=False)
#         assert n == 2

#         n, r, sr, f = dq.get(block=True)
#         assert n == 2



    
