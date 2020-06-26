from queue import Empty
from multiprocessing import Manager, Queue
from uuid import uuid1
import logging
from tx.functional.either import Left, Right
from tx.readable_log import format_message, getLogger

logger = getLogger(__name__, logging.INFO)

class Node:
    def __init__(self, o, node_id=None, ret=None, depends_on=set(), subnode_depends_on=set()):
        """
        :param o: The task object
        :param node_id: The node_id, if None, one will be generated
        :type node_id: maybe[str]
        :param depends_on: a set of node_ids that it depends
        :type depends_on: set[str]
        :param subnode_depends_on: a set of node_ids that it depends
        :type subnode_depends_on: set[str]
        """
        self.o = o
        self.node_id = node_id if node_id is not None else str(uuid1())
        self.ret = ret
        self.depends_on = depends_on
        self.subnode_depends_on = subnode_depends_on

    def get(self):
        return self.o

    def __eq__(self, other):
        return self.o == other.o and self.node_id == other.node_id and self.ret == other.ret and self.depends_on == other.depends_on

    
class NodeMetadata:
    """
    :attr refs: a map from node_id to node_ids that depends on it
    :type refs: dict[str, iterable[str]]
    :attr subnode_refs: a map from node_id to node_ids whose subnode depends on it
    :type subnode_refs: dict[str, iterable[str]]
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
    def __init__(self, refs=set(), subnode_refs=set(), depends=set(), subnode_depends=set(), results={}, subnode_results={}):
        self.refs = refs
        self.subnode_refs = subnode_refs
        self.depends = depends
        self.subnode_depends = subnode_depends
        self.results = results
        self.subnode_results = subnode_results

        
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
    :attr outputs: output of the script
    :type outputs: dict[str, any]
    :attr end_of_queue: an end_of_queue object that will be put on the ready queue when the NodeMap is closed. 
    :type end_of_queue: any
    """
    
    def __init__(self, manager, end_of_queue):
        self.meta = manager.dict()
        self.nodes = manager.dict()
        self.ready_queue = manager.Queue()
        self.lock = manager.Lock()
        self.outputs = manager.dict()
        self.end_of_queue = end_of_queue

    def close(self):
        with self.lock:
            self.ready_queue.put((Node(self.end_of_queue), {}))

    # :param is_hold: whether the node is a hold node. a hold node will not be added to the ready queue, it is used for holding a sequence of nodes that are just added, preventing them from being added to ready queue.
    # :type is_hold: boolean
    def add_node(self, node, is_hold=False):
        with self.lock:
            if node.node_id in self.nodes:
                raise RuntimeError(f"{node.node_id} is already in the map")
            
            self.nodes[node.node_id] = node
            self.meta[node.node_id] = NodeMetadata(depends=node.depends_on, subnode_depends=node.subnode_depends_on)
            logger.info(f"add_node: {node.node_id} depends_on {node.depends_on} subnode_depends_on {node.subnode_depends_on}")
            for node_id in node.depends_on:
                meta = self.meta.get(node_id, NodeMetadata())
                meta.refs.add(node.node_id)
                self.meta[node_id] = meta
            for node_id in node.subnode_depends_on:
                meta = self.meta.get(node_id, NodeMetadata())
                meta.subnode_refs.add(node.node_id)
                self.meta[node_id] = meta
            if not is_hold and len(node.depends_on) == 0:
                self.ready_queue.put((node, {}))

    def complete_node(self, node_id, ret, result):
        with self.lock:
            logger.info(f"complete_node: {node_id} complete, ret = {ret}")
            node = self.nodes[node_id]
            meta = self.meta[node_id]
            refs = meta.refs
            subnode_refs = meta.subnode_refs
            logger.info(f"complete_node: refs = {refs}")

            for ref in subnode_refs:
                refmeta = self.meta[ref]
                refmeta.subnode_depends.remove(node_id)
                refmeta.subnode_results[node_id] = result
                self.meta[ref] = refmeta
                logger.info(f"complete_node: subnode ref = {ref}, refdep = {refmeta.subnode_depends}, refresults = {refmeta.subnode_results}")
                    
            for ref in refs:
                refmeta = self.meta[ref]
                refmeta.depends.remove(node_id)
                refmeta.results[node_id] = result
                self.meta[ref] = refmeta
                logger.info(f"complete_node: ref = {ref}, refdep = {refmeta.depends}, refresults = {refmeta.results}")

                if len(refmeta.depends) == 0:
                    task = (self.nodes[ref], refmeta.results)
                    logger.info(f"complete_node: ref = {ref}, len(refdep) == 0, task = {task}")
                    self.ready_queue.put(task)
            logger.info(f"complete_node: updating outputs with {ret}")
            self.outputs.update(ret)
            del self.meta[node_id]
            del self.nodes[node_id]

    def get_next_ready_node(self, *args, **kwargs):
        logger.info(f"NodeMap.get_next_ready_node: self.ready_queue.qsize() = {self.ready_queue.qsize()} len(self.nodes) = {len(self.nodes)}")
        return self.ready_queue.get(*args, **kwargs)

    def empty(self):
        with self.lock:
            return len(self.nodes) == 0

        
class DependentQueue:
    def __init__(self, manager, end_of_queue):
        self.node_map = NodeMap(manager, end_of_queue)

    def put(self, o, job_id=None, ret=[], depends_on=set(), subnode_depends_on=set(), is_hold=False):
        node = Node(o, node_id=job_id, ret=ret, depends_on=depends_on, subnode_depends_on=subnode_depends_on)
        self.node_map.add_node(node, is_hold=is_hold)
        return node.node_id

    def get(self, *args, **kwargs):
        node, results = self.node_map.get_next_ready_node(*args, **kwargs)
        logger.info(f"DependentQueue.get: node = {node}, results = {results}")
        return node.get(), results, node.node_id
        
    def complete(self, node_id, ret, x=None):
        self.node_map.complete_node(node_id, ret, x)
        if self.node_map.empty():
            self.node_map.close()

    def get_results(self):
        return dict(self.node_map.outputs)


class SubQueue:
    def __init__(self, queue):
        self.queue = queue
        self.subqueue = Queue()

    def put(self, o, job_id=None, ret=[], depends_on=set(), subnode_depends_on=set(), is_hold=False):
        return self.queue.put(o, job_id, ret, depends_on, subnode_depends_on, is_hold=is_hold)
    
    def put_in_subqueue(self, o):
        self.subqueue.put(o)

    def get(self, *args, **kwargs):
        return self.subqueue.get(*args, **kwargs)

    def complete(self, node_id, ret, x=None):
        self.queue.complete(node_id, ret, x)

    def full(self):
        return self.subqueue.full()
        
        
