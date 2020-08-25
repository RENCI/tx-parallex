import logging
from tx.functional.either import Left, Right
from tx.readable_log import getLogger, format_message

logger = getLogger(__name__, logging.INFO)



def inverse_function(func):
    inv_func = {}
    for k, v in func.items():
        ks = inv_func.get(v, [])
        inv_func[v] = ks + [k]
    return inv_func


def mappend(a, b):
    ac = str(a)
    bc = str(b)
    if isinstance(a, Right) and isinstance(b, Right):
        ret = Right(mappend(a.value, b.value))
    elif isinstance(a, Left):
        ret = a
    elif isinstance(b, Left):
        ret = b
    elif isinstance(a, list) and isinstance(b, list):
        a.extend(b)
        ret = a
    elif isinstance(a, dict) and isinstance(b, dict):
        for k, v in b.items():
            a[k] = mappend(a.get(k), v)
        ret = a
    else:
        ret = b

    return ret
    

    
