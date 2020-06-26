import logging

logger = logging.getLogger(__name__)


def inverse_function(func):
    inv_func = {}
    for k, v in func.items():
        ks = inv_func.get(v, [])
        inv_func[v] = ks + [k]
    return inv_func


        
        
    
