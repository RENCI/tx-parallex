from multiprocessing import Manager
from queue import Queue, Empty
import pytest
from parallex.dependentqueue import DependentQueue, Node


def test_dep():
    with Manager() as manager:
        dq = DependentQueue(manager)

        id3 = dq.put(3)
        id2 = dq.put(2, depends_on={"c": id3})
        id1 = dq.put(1, depends_on={"a": id2,"b": id3})
        
        n, r, f = dq.get(block=False)
        assert n == 3
        assert r == {}
        f(6)
        n, r, f = dq.get(block=False)
        assert n == 2
        assert r == {"c": 6}
        f(5)
        n, r, f = dq.get(block=False)
        assert n == 1
        assert r == {"a": 5, "b": 6}
        f(4)
        with pytest.raises(Empty):
            dq.get(block=False)
    
