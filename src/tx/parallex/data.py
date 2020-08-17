from dataclasses import dataclass

def _is(a, b):
    return a is b

def _is_not(a, b):
    return a is not b

def _eq(a, b):
    return a == b

def _not_eq(a, b):
    return a != b

def _lt(a, b):
    return a < b

def _gt(a, b):
    return a > b

def _lt_e(a, b):
    return a <= b

def _gt_e(a, b):
    return a >= b

def _in(a, b):
    return a in b

def _not_in(a, b):
    return a not in b

def _and(*a):
    return all(a)

def _or(*a):
    return any(a)

def _add(a, b):
    return a + b

def _sub(a, b):
    return a - b

def _mult(a, b):
    return a * b

def _div(a, b):
    return a / b

def _mod(a, b):
    return a % b

def _floor_div(a, b):
    return a // b

def _l_shift(a, b):
    return a << b

def _r_shift(a, b):
    return a >> b

def _bit_and(a, b):
    return a & b

def _bit_or(a, b):
    return a | b

def _bit_xor(a, b):
    return a ^ b

def _mat_mult(a, b):
    return a @ b

def _pow(a, b):
    return a ** b

def _invert(a):
    return ~a

def _not(a):
    return not a

def _u_add(a):
    return +a

def _u_sub(a):
    return -a

def _if_exp(t,b,o):
    return b if t else o

def _subscript(a, b):
    return a[b]

@dataclass
class Starred:
    def __init__(self, a):
        self.value = a

def _starred(a):
    return Starred(a)

def _list(*args):
    l = []
    for arg in args:
        if isinstance(arg, Starred):
            l.extend(arg.value)
        else:
            l.append(arg)
            
    return l

def _tuple(*args):
    return args

def _dict(*args):
    n = int(len(args)/2)
    obj = {}
    for k, v in zip(args[:n], args[n:]):
        if k is None:
            obj.update(v)
        else:
            obj[k] = v
    return obj
