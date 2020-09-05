import logging
import json
from .utils import mappend
from tx.functional.maybe import Nothing
from tx.readable_log import format_message, getLogger
from typing import List, Any, Dict, Tuple, Set, Callable, TypeVar
import jsonpickle

logger = getLogger(__name__, logging.INFO)

def write_to_disk(dqueue, path):
    with open(path, "w") as db:
        while True:
            output = dqueue.get_next_output()
            if output == Nothing:
                break
            else:
                db.write(jsonpickle.encode(output.value) + "\n")


def merge_files(inputs, path):
    logger.info(format_message("merge_files","merge files", {"into": path}))
    with open(path, "wb") as db:
        for inp in inputs:
            logger.info(format_message("merge_files", "merge a file", {"from": inp}))
            with open(inp, "rb") as inp_file:
                
                while True:
                    buf = inp_file.read(1024 * 1024)
                    if len(buf) == 0:
                        break
                    db.write(buf)


def read_from_disk(path):
    obj = {}
    with open(path) as db:
        for line in db:
            obj = mappend(obj, jsonpickle.decode(line))
    return obj

        
