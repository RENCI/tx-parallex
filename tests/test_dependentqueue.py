from multiprocessing import Manager
from queue import Queue, Empty
import pytest
from tx.parallex.dependentqueue import DependentQueue, Node


def test_dep():
    with Manager() as manager:
        dq = DependentQueue(manager)

        id3 = dq.put(3)
        id2 = dq.put(2, depends_on={id3: ["c"]})
        id1 = dq.put(1, depends_on={id2: ["a"], id3: ["b"]})
        
        n, r, f = dq.get(block=False)
        assert n == 3
        assert r == {}
        dq.complete(f, 6)
        n, r, f = dq.get(block=False)
        assert n == 2
        assert r == {"c": 6}
        dq.complete(f, 5)
        n, r, f = dq.get(block=False)
        assert n == 1
        assert r == {"a": 5, "b": 6}
        dq.complete(f, 4)
        with pytest.raises(Empty):
            dq.get(block=False)
    
