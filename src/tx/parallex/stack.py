from itertools import chain
import logging

logger = logging.getLogger(__name__)

def Stack(base):
    class _Stack:
        def __init__(self, prev = base, curr = base):
            self.prev = prev.copy()
            self.curr = curr.copy()

        def __getitem__(self, key):
            if isinstance(key, int):
                if key < len(self.prev):
                    return self.prev[key]
                else:
                    return self.curr[key - len(self.prev)]
            else:
                if key in self.curr:
                    return self.curr[key]
                else:
                    return self.prev[key]

        def __setitem__(self, key, value):
            self.curr[key] = value

        def __contains__(self, elt):
            return elt in self.curr or elt in self.prev

        def __or__(self, iterable):
            return _Stack(self.prev, self.curr | iterable)

        def __str__(self):
            return f"{self.curr}>{self.prev}"

        def __len__(self):
            return len(self.curr) + len(self.prev)

        def __iter__(self):
            return chain(iter(self.prev), iter(self.curr))

        def keys(self):
            return self.curr.keys() | self.prev.keys()

        def copy(self):
            return _Stack(self.prev, self.curr)
            

    return _Stack

    
