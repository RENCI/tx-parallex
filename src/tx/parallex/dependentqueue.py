from uuid import uuid1
import logging
from multiprocessing import Manager
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import List, Any, Dict, Tuple, Callable, Set, Optional
import time
import datetime
from tx.functional.either import Left, Right, Either
from tx.functional.maybe import Just, Nothing, Maybe
from tx.readable_log import format_message, getLogger
from .data import Starred
from .objectstore import ObjectStore

logger = getLogger(__name__, logging.INFO)


ReturnType = Dict[str, Either[Any, Any]]

ResultType = Either[Any, ReturnType]

DTask = Tuple[Any, ReturnType, ReturnType, str]

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
    names: Set[str]
    depends_on: Dict[str, Set[str]] = field(default_factory=dict)
    subnode_depends_on: Dict[str, Set[str]] = field(default_factory=dict)
    start_time: float = -1
    ready_time: float = -1

    def get(self) -> Any:
        return self.o


TaskType = Node


@dataclass
class NodeMetadata:
    """
    :attr refs: a set of node_ids that depends on it
    :attr subnode_refs: a set of node_ids whose subnode depends on it
    :attr depends: a map from node_id to a dict where the key is node_id that it depends on and the value is an iterable that returns a list of keys
    :attr subnode_depends: a map from node_id to a dict where the key is node_id that its subnodes depends on and the value is an iterable that returns a list of keys
    """
    refs: Set[str] = field(default_factory=set)
    subnode_refs: Set[str] = field(default_factory=set)
    depends: int = 0
    subnode_depends: int = 0


def gen_oid(node_id: str, name: str) -> str:
    return f"{node_id}/{name}"

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
    :attr end_of_queue: an end_of_queue object that will be put on the ready queue when the NodeMap is closed. The object must implement the __eq__ method such that every copy of the object is equal to any other copy.
    :type end_of_queue: any
    """
    
    def __init__(self, manager: Manager, end_of_queue: Any, object_store: ObjectStore):
        self.meta = manager.dict()
        self.nodes = manager.dict()
        self.ready_queue = manager.Queue()
        self.lock = manager.Lock()
        self.node_lock = manager.dict()
        self.end_of_queue = end_of_queue
        self.manager = manager
        self.node_ready_time = manager.dict()
        self.node_start_time = manager.dict()
        self.object_store = object_store

    def init_thread(self) -> None:
        self.object_store.init_thread()        

    def get_node_lock(self, node_id: str):
        with self.lock:
            lock = self.node_lock.get(node_id)
            if lock is None:
                lock = self.node_lock[node_id] = self.manager.Lock()
            else:
                lock = self.node_lock[node_id]
            return lock

    @contextmanager
    def node_metadata(self, node_id: str) -> None:
        with self.get_node_lock(node_id):
            meta = self.meta.get(node_id, NodeMetadata())
            yield meta
            self.meta[node_id] = meta
    
    # :param is_hold: whether the node is a hold node. a hold node will not be added to the ready queue, it is used for holding a sequence of nodes that are just added, preventing them from being added to ready queue.
    # :type is_hold: boolean
    def add_node(self, node: Node, is_hold: bool =False) -> None:
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
            self.put_ready_queue(node)

    def put_ready_queue(self, task: TaskType) -> None:
        node = task
        logger.info(f"task added to ready queue {node.node_id}")
        self.node_ready_time[node.node_id] = time.time()
        self.ready_queue.put(task)

    # :param result: the result of the function, if it is Nothing then no result is returned
    def complete_node(self, node_id: str, result: ResultType) -> None:
        node_complete_time = time.time()
        logger.debug(format_message("complete_node", node_id, {"result": result}))
        
        node = self.nodes[node_id]
        meta = self.meta[node_id]
        names = node.names # the vars returned by this node
        refs = meta.refs
        subnode_refs = meta.subnode_refs
        logger.debug(format_message("complete_node", node_id, {"refs": refs, "subnode_refs": subnode_refs}))

        oids : Set[str] = set()

        logger.debug(format_message("complete_node", node_id, {"names": names}))
        # put results in object store
        if isinstance(result, Left):
            for name in names:
                oid = gen_oid(node_id, name)
                self.object_store.put(oid, result)
                # Increment reference count so that other processes cannot reduce the ref count to 0 before we finished adding them to all results or subnode_results of their refs and subnode_refs.
                self.object_store.increment_ref(oid)
                oids.add(oid)
        else:
            result_dict = result.value
            for name in names:
                oid = gen_oid(node_id, name)
                self.object_store.put(oid, result_dict[name])
                # same as above
                self.object_store.increment_ref(oid)
                oids.add(oid)
        
        for ref in subnode_refs | refs:
            with self.node_metadata(ref) as refmeta:
                refnode = self.nodes[ref]
                oid_incr : Dict[str, int] = {}
                if ref in subnode_refs:
                    refnode_subnode_depends_on = refnode.subnode_depends_on[node_id]
                    for name in refnode_subnode_depends_on:
                        oid = gen_oid(node_id, name)
                        oid_incr[oid] = oid_incr.get(oid, 0) + 1
                    refmeta.subnode_depends -= 1
                if ref in refs:
                    refnode_depends_on = refnode.depends_on[node_id]
                    for name in refnode_depends_on:
                        oid = gen_oid(node_id, name)
                        oid_incr[oid] = oid_incr.get(oid, 0) + 1
                    refmeta.depends -= 1

                self.object_store.update_refs(oid_incr)

                if refmeta.depends == 0 and refmeta.subnode_depends == 0:
                    task = self.nodes[ref]
                    # If we didn't increment the ref count of oids that are used in this task when those oids are generated, some other process that grabs this task could reduce the ref count to 0 and cause the object to be deleted from the object store before we finish adding it to other refs and subnode_refs.
                    # Adding tasks only when all refs and subnode_refs are added to will not work without locking the lock because more than one processes may progress to this block at the same time causing creating duplicate tasks.
                    self.put_ready_queue(task)

        for oid in oids:
            self.object_store.decrement_ref(oid)

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
                "complete_to_finish": node_finish_time - node_complete_time,
                "remaining (estimate)": lambda: len(self.nodes),
                "ready (estimate)": lambda: self.ready_queue.qsize()
            }))

        

    def get_next_ready_node(self, *args, **kwargs) -> TaskType:
        logger.debug("NodeMap.get_next_ready_node: self.ready_queue.qsize() = %s len(self.nodes) = %s", self.ready_queue.qsize(), len(self.nodes))
        node = self.ready_queue.get(*args, **kwargs)
        logger.debug("NodeMap.get_next_ready_node: node = %s self.end_of_queue = %s", node, self.end_of_queue)
        if node.o == self.end_of_queue:
            self.ready_queue.put(node)
        self.node_start_time[node.node_id] = time.time()
        return node

    def close(self) -> None:
        self.ready_queue.put(Node(self.end_of_queue, f"end_of_queue@{uuid1()}", set()))

    # def empty(self):
    #     with self.lock:
    #         logger.debug("empty: len(self.nodes) == %s", len(self.nodes))
    #         return len(self.nodes) == 0

        
class DependentQueue:
    """The queue maintain a list of tasks. Before any task is added to the list, the queue is in the ready state, when the last task is compleete the queue is in the closed state. In the closed state the queue will always return end_of_queue.
    """
    def __init__(self, manager: Manager, end_of_queue: Any, object_store: ObjectStore):
        self.node_map = NodeMap(manager, end_of_queue, object_store)

    def init_thread(self) -> None:
        self.node_map.init_thread()
        
    def put(self, o : Any, job_id:Optional[str]=None, depends_on:Dict[str, Set[str]]={}, subnode_depends_on:Dict[str, Set[str]]={}, names:Set[str]=set(), is_hold: bool=False) -> str:
        if job_id is None:
            job_id =  str(uuid1())
        logger.info(format_message("DependentQueue.put", "putting a task on the queue", {"task_id": job_id, "depends_on": depends_on, "subnode_depends_on": subnode_depends_on, "names": names, "is_hold": is_hold}))
        node = Node(o, node_id=job_id, depends_on=depends_on, subnode_depends_on=subnode_depends_on, names=names)
        self.node_map.add_node(node, is_hold=is_hold)
        return node.node_id

    def get(self, *args, **kwargs) -> DTask:
        def retrieve_object(oid: str) -> Any:
            obj = self.node_map.object_store.get(oid)
            self.node_map.object_store.decrement_ref(oid)
            return obj
        
        def retrieve_objects(result_oid_dict : Dict[str, Set[str]]) -> Dict[str, Any]:
            return {k: retrieve_object(gen_oid(v, k)) for v,ks in result_oid_dict.items() for k in ks}
            
        node = self.node_map.get_next_ready_node(*args, **kwargs)
        results = retrieve_objects(node.depends_on)
        subnode_results = retrieve_objects(node.subnode_depends_on)
        logger.debug(f"DependentQueue.get: node = %s, results = %s, subnode_results = %s", node, results, subnode_results)
        return node.get(), results, subnode_results, node.node_id
        
    def complete(self, node_id: str, x: ResultType) -> None:
        self.node_map.complete_node(node_id, x)

    def close(self) -> None:
        self.node_map.close()
        
        
