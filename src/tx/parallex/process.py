import sys
from queue import Queue
from uuid import uuid1
from random import choice
from enum import Enum
from importlib import import_module
from itertools import chain
from more_itertools import roundrobin
import logging
import traceback
from graph import Graph
from functools import partial
from copy import deepcopy
from ctypes import c_int
import builtins
from joblib import Parallel, delayed, parallel_backend
import os
from tx.functional.either import Left, Right, Either
from tx.functional.maybe import Just, Nothing, maybe
from .dependentqueue import DependentQueue
from .utils import inverse_function
from .python import python_to_spec
from .stack import Stack
import jsonpickle
from tx.readable_log import format_message, getLogger
from typing import List, Any, Dict, Tuple, Set, Callable, TypeVar
from .dependentqueue import DependentQueue
from .task import EndOfQueue

logger = getLogger(__name__, logging.INFO)

def work_on(queue : DependentQueue, library_paths : List[str]) -> None:
    logger.debug("library_paths = %s", library_paths)
    sys.path.extend(library_paths)
    queue.init_thread()
    while True:
        jri = queue.get()
        job, results, subnode_results, jid = jri
        if isinstance(job, EndOfQueue):
            break
        else:
            logger.debug(format_message("work_on", "task begin.", {
                "job": job,
                "jid": jid,
                "params": results
            }))
            logger.info("task begin %s", jid)
            ret, resultj = job.run(results, subnode_results, queue)
            logger.debug(format_message("work_on", "task complete.", {
                "job": job,
                "jid": jid,
                "ret": ret,
                "resultj": resultj,
                "params": results
            }))
            logger.info(f"task finish %s", jid)
            queue.complete(jid, ret, resultj)
    


    

    

        
        
    
