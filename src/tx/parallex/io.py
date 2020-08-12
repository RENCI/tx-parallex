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
from dataclasses import dataclass
from abc import ABC, abstractmethod

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
            obj.update(jsonpickle.decode(line))
    return obj

        
