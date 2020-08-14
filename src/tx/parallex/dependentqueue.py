from queue import Empty, Queue
from threading import Thread, Lock
from uuid import uuid1
from ctypes import c_bool
import logging
from tx.functional.either import Left, Right, Either
from tx.functional.maybe import Just, Nothing
from tx.readable_log import format_message, getLogger
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import List, Any, Dict, Tuple, Callable, Set, Optional
import time
import datetime
from ctypes import c_int
import numpy as np
from tx.parallex.data import Starred
from .objectstore import ObjectStore

logger = getLogger(__name__, logging.INFO)

@dataclass
class Node:
    """
    :param o: The task object
    :param node_id: The node_id, if None, one will be generated
    :param depends_on: a dict from node_ids that it depends to variable names
    :param subnode_depends_on: a dict from node_ids that it depends to variable names
    """
    o: Any
    node_id: str
    depends_on: Dict[str, Set[str]] = field(default_factory=dict)
    subnode_depends_on: Dict[str, Set[str]] = field(default_factory=dict)
    start_time: float = -1
    ready_time: float = -1

    def get(self):
        return self.o

    
@dataclass
class NodeMetadata:
    """
    :attr refs: a set of node_ids that depends on it
    :type refs: Set[str]
    :attr subnode_refs: a set of node_ids whose subnode depends on it
    :type subnode_refs: Set[str]
    :attr depends: a map from node_id to a dict where the key is node_id that it depends on and the value is an iterable that returns a list of keys
    :type depends: dict[str, iterable[str]]
    :attr subnode_depends: a map from node_id to a dict where the key is node_id that its subnodes depends on and the value is an iterable that returns a list of keys
    :type subnode_depends: dict[str, iterable[str]]
    :attr nodes:
    :type nodes: dict[str, Node]
    :attr results: a map from node_id to partial or complete results of the nodes that it depends on. These object are store in memory. To ignore the result, set dependencies to empty iterable.
    :type results: dict[str, dict[str, any]]
    :attr subnode_results: a map from node_id to partial or complete results of the nodes that its subnodes depends on. These object are store in memory. To ignore the result, set dependencies to empty iterable.
    :type subnode_results: dict[str, dict[str, any]]
    """
    refs: Set[str] = field(default_factory=set)
    subnode_refs: Set[str] = field(default_factory=set)
    depends: int = 0
    subnode_depends: int = 0
    results: Dict[str, Either] = field(default_factory=dict)
    subnode_results: Dict[str, Either] = field(default_factory=dict)


class NodeMap:
    """
    :attr nodes:
    :type nodes: dict[str, Node]
    :attr meta: a map from node_id to its metadata
    :type meta: dict[str, NodeMetadata]
    :attr ready_queue: a queue of tasks that are ready
    :type ready_queue: Queue[Node]
    :attr lock: global lock
    :type lock: Lock
    :attr output_queue: output of the script
    :type output_queue: Queue[dict[str, any]]
    :attr end_of_queue: an end_of_queue object that will be put on the ready queue when the NodeMap is closed. The object must implement the __eq__ method such that every copy of the object is equal to any other copy.
    :type end_of_queue: any
    """
    
    def __init__(self, manager, end_of_queue, plasma_store):
        self.meta = manager.dict()
        self.nodes = manager.dict()
        self.ready_queue = manager.Queue()
        self.lock = manager.Lock()
        self.node_lock = manager.dict()
        self.output_queue = manager.Queue()
        self.end_of_queue = end_of_queue
        self.manager = manager
        self.node_ready_time = manager.dict()
        self.node_start_time = manager.dict()
        self.object_store = ObjectStore(manager, plasma_store)

    def init_thread(self):
        self.object_store.init_thread()        

    def get_node_lock(self, node_id):
        with self.lock:
            lock = self.node_lock.get(node_id)
            if lock is None:
                lock = self.node_lock[node_id] = self.manager.Lock()
            else:
                lock = self.node_lock[node_id]
            return lock

    @contextmanager
    def node_metadata(self, node_id):
        with self.get_node_lock(node_id):
            meta = self.meta.get(node_id, NodeMetadata())
            yield meta
            self.meta[node_id] = meta
    
    # :param is_hold: whether the node is a hold node. a hold node will not be added to the ready queue, it is used for holding a sequence of nodes that are just added, preventing them from being added to ready queue.
    # :type is_hold: boolean
    def add_node(self, node: Node, is_hold: bool =False):
        with self.get_node_lock(node.node_id):
            if node.node_id in self.nodes:
                raise RuntimeError(f"{node.node_id} is already in the map")
            self.nodes[node.node_id] = node

        with self.node_metadata(node.node_id) as meta:
            meta.depends = len(node.depends_on)
            meta.subnode_depends = len(node.subnode_depends_on)

        logger.info("add_node: %s", node.node_id)
        logger.debug("add_node: %s depends_on %s subnode_depends_on %s", node.node_id, node.depends_on, node.subnode_depends_on)
        for node_id in node.depends_on.keys():
            with self.node_metadata(node_id) as meta:
                meta.refs.add(node.node_id)
                logger.debug(format_message("add_node", lambda: f"add {node.node_id} to refs of {node_id}", lambda: vars(meta)))

        for node_id in node.subnode_depends_on.keys():
            with self.node_metadata(node_id) as meta:
                meta.subnode_refs.add(node.node_id)
                logger.debug(format_message("add_node", lambda: f"add {node.node_id} to subnode refs of {node_id}", lambda: vars(meta)))

        if not is_hold and len(node.depends_on) == 0:
            self.put_ready_queue((node, {}, {}))

    def put_ready_queue(self, task):
        node, _, _ = task
        logger.info(f"task added to ready queue {node.node_id}")
        self.node_ready_time[node.node_id] = time.time()
        self.ready_queue.put(task)

    # :param result: the result of the function, if it is Nothing then no result is returned
    def complete_node(self, node_id: str, ret: Dict[str, Any], result: Either):
        node_complete_time = time.time()
        logger.debug(format_message("complete_node", node_id, {"ret": ret, "result": result}))
        
        node = self.nodes[node_id]
        meta = self.meta[node_id]
        refs = meta.refs
        subnode_refs = meta.subnode_refs
        logger.debug(format_message("complete_node", node_id, {"refs": refs, "subnode_refs": subnode_refs}))

        oids : Set[ObjectID] = set()
        
        # put results in object store
        if isinstance(result, Left):
            oid = self.object_store.put(result)
            result_oid = lambda _: oid
            # Increment reference count so that other processes cannot reduce the ref count to 0 before we finished adding them to all results or subnode_results of their refs and subnode_refs.
            self.object_store.increment_ref(oid)
            oids.add(oid)
        else:
            result_dict = result.value
            result_oid_dict = {}
            for k, v in result_dict.items():
                void = self.object_store.put(v)
                result_oid_dict[k] = void
                # same as above
                self.object_store.increment_ref(void)
                oids.add(void)
            result_oid = lambda x: result_oid_dict[x]
        
        for ref in subnode_refs | refs:
            with self.node_metadata(ref) as refmeta:
                refnode = self.nodes[ref]
                if ref in subnode_refs:
                    refnode_subnode_depends_on = refnode.subnode_depends_on[node_id]
                    for name in refnode_subnode_depends_on:
                        oid = result_oid(name)
                        refmeta.subnode_results[name] = oid
                        self.object_store.increment_ref(oid)
                    refmeta.subnode_depends -= 1
                if ref in refs:
                    refnode_depends_on = refnode.depends_on[node_id]
                    for name in refnode_depends_on:
                        oid = result_oid(name)
                        refmeta.results[name] = oid
                        self.object_store.increment_ref(oid)
                    refmeta.depends -= 1

                if refmeta.depends == 0 and refmeta.subnode_depends == 0:
                    task = (self.nodes[ref], refmeta.results, refmeta.subnode_results)
                    # If we didn't increment the ref count of oids that are used in this task when those oids are generated, some other process that grabs this task could reduce the ref count to 0 and cause the object to be deleted from the object store before we finish adding it to other refs and subnode_refs.
                    # Adding tasks only when all refs and subnode_refs are added to will not work without locking the lock because more than one processes may progress to this block at the same time causing creating duplicate tasks.
                    self.put_ready_queue(task)

        for oid in oids:
            self.object_store.decrement_ref(oid)

        logger.debug("complete_node: putting %s on output_queue", ret)
        self.put_output(ret)
            
        with self.lock:
            logger.debug("complete_node: deleting %s from self.meta", node_id)
            del self.meta[node_id]
            del self.nodes[node_id]
            logger.debug("complete_node: len(self.nodes) = %s", len(self.nodes))
            if len(self.nodes) == 0:
                self.close()

        node_finish_time = time.time()
        nrt = self.node_ready_time.get(node_id)
        nst = self.node_start_time.get(node_id)
        if nrt is None:
            logger.info(format_message("NodeMap.complete_node", "node ready time does not exist", {
                "node_id": node_id,
            }))            
        elif nst is None:
            logger.info(format_message("NodeMap.complete_node", "node start time does not exist", {
                "node_id": node_id,
            }))
        else:
            logger.info(format_message("NodeMap.complete_node", "time", {
                "node_id": node_id,
                "ready_time": datetime.datetime.fromtimestamp(nrt),
                "start_time": datetime.datetime.fromtimestamp(nst),
                "complete_time": datetime.datetime.fromtimestamp(node_complete_time),
                "finish_time": datetime.datetime.fromtimestamp(node_finish_time),
                "ready_to_start": nst - nrt,
                "start_to_complete": node_complete_time - nst,
                "complete_to_finish": node_finish_time - node_complete_time
            }))

        

    def get_next_ready_node(self, *args, **kwargs):
        logger.debug("NodeMap.get_next_ready_node: self.ready_queue.qsize() = %s len(self.nodes) = %s", self.ready_queue.qsize(), len(self.nodes))
        node, results, subnode_results = self.ready_queue.get(*args, **kwargs)
        logger.debug("NodeMap.get_next_ready_node: node = %s self.end_of_queue = %s", node, self.end_of_queue)
        if node.o == self.end_of_queue:
            self.ready_queue.put((node, results, subnode_results))
        self.node_start_time[node.node_id] = time.time()
        return node, results, subnode_results

    def get_next_output(self):
        return self.output_queue.get()

    def put_output(self, o):
        self.output_queue.put(Just(o))

    def close(self):
        self.ready_queue.put((Node(self.end_of_queue, f"end_of_queue@{uuid1()}"), {}, {}))
        self.output_queue.put(Nothing)

    # def empty(self):
    #     with self.lock:
    #         logger.debug("empty: len(self.nodes) == %s", len(self.nodes))
    #         return len(self.nodes) == 0

        
class DependentQueue:
    """The queue maintain a list of tasks. Before any task is added to the list, the queue is in the ready state, when the last task is compleete the queue is in the closed state. In the closed state the queue will always return end_of_queue.
    """
    def __init__(self, manager, end_of_queue, plasma_store = "/tmp/txparallex"):
        self.node_map = NodeMap(manager, end_of_queue, plasma_store)

    def init_thread(self):
        self.node_map.init_thread()
        
    def put(self, o : Any, job_id:Optional[str]=None, depends_on:Dict[str, Set[str]]={}, subnode_depends_on:Dict[str, Set[str]]={}, is_hold: bool=False):
        if job_id is None:
            job_id =  str(uuid1())
        logger.info(format_message("DependentQueue.put", "putting a task on the queue", {"task_id": job_id, "depends_on": depends_on, "subnode_depends_on": subnode_depends_on, "is_hold": is_hold}))
        node = Node(o, node_id=job_id, depends_on=depends_on, subnode_depends_on=subnode_depends_on)
        self.node_map.add_node(node, is_hold=is_hold)
        return node.node_id

    def get(self, *args, **kwargs):
        def retrieve_object(oid):
            obj = self.node_map.object_store.get(oid)
            self.node_map.object_store.decrement_ref(oid)
            return obj
        
        def retrieve_objects(result_oid_dict):
            return {k: retrieve_object(v) for k,v in result_oid_dict.items()}
            
        node, results, subnode_results = self.node_map.get_next_ready_node(*args, **kwargs)
        logger.debug(f"DependentQueue.get: node = %s, results = %s, subnode_results = %s", node, results, subnode_results)
        return node.get(), retrieve_objects(results), retrieve_objects(subnode_results), node.node_id
        
    def get_next_output(self):
        return self.node_map.get_next_output()

    def put_output(self, o):
        self.node_map.put_output(o)
    
    def complete(self, node_id, ret, x=Nothing):
        self.node_map.complete_node(node_id, ret, x)

    def close(self):
        self.node_map.close()
        
        
