import sys
import logging
from .utils import inverse_function, mappend
import jsonpickle
from tx.functional.maybe import Nothing
from tx.readable_log import format_message, getLogger
from typing import List, Any, Dict, Tuple, Set, Callable, TypeVar

logger = getLogger(__name__, logging.INFO)

def write_to_disk(dqueue, path):
    with open(path, "w") as db:
        while True:
            output = dqueue.get_next_output()
            if output == Nothing:
                break
            else:
                db.write(jsonpickle.encode(output.value) + "\n")


def read_from_disk(path):
    obj = {}
    with open(path) as db:
        for line in db:
            obj = mappend(obj, jsonpickle.decode(line))
    return obj

        
