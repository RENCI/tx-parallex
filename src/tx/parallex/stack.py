def Stack(base):
    class _Stack:
        def __init__(self, prev = base, curr = base):
            self.prev = prev
            self.curr = curr

        def __getitem__(self, key):
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
            

    return _Stack

    
