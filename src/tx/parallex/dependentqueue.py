from queue import Empty, Queue
from threading import Thread, Lock
from uuid import uuid1
from ctypes import c_bool
import logging
from tx.functional.either import Left, Right
from tx.functional.maybe import Just, Nothing
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
        return isinstance(other, Node) and self.o == other.o and self.node_id == other.node_id and self.ret == other.ret and self.depends_on == other.depends_on

    
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
    def __init__(self, refs=None, subnode_refs=None, depends=0, subnode_depends=0, results=None, subnode_results=None):
        self.refs = refs if refs is not None else set()
        self.subnode_refs = subnode_refs if subnode_refs is not None else set()
        self.depends = depends
        self.subnode_depends = subnode_depends
        self.results = results if results is not None else {}
        self.subnode_results = subnode_results if subnode_results is not None else {}

    def decrement_depends(self):
        self.depends -= 1
            
    def decrement_subnode_depends(self):
        self.subnode_depends -= 1


        
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
    
    def __init__(self, manager, end_of_queue):
        self.meta = manager.dict()
        self.nodes = manager.dict()
        self.ready_queue = manager.Queue()
        self.lock = manager.Lock()
        self.node_lock = manager.dict()
        self.output_queue = manager.Queue()
        self.end_of_queue = end_of_queue
        self.manager = manager

    # :param is_hold: whether the node is a hold node. a hold node will not be added to the ready queue, it is used for holding a sequence of nodes that are just added, preventing them from being added to ready queue.
    # :type is_hold: boolean
    def add_node(self, node, is_hold=False):
        with self.lock:
            if node.node_id in self.node_lock:
                raise RuntimeError(f"{node.node_id} is already in the map")
            self.node_lock[node.node_id] = self.manager.Lock()
            self.nodes[node.node_id] = node
            self.meta[node.node_id] = NodeMetadata(depends=len(node.depends_on), subnode_depends=len(node.subnode_depends_on))

        logger.info("add_node: %s", node.node_id)
        logger.debug("add_node: %s depends_on %s subnode_depends_on %s", node.node_id, node.depends_on, node.subnode_depends_on)
        for node_id in node.depends_on:
            with self.node_lock[node_id]:
                meta = self.meta.get(node_id, NodeMetadata())
                logger.debug(format_message("add_node", "before add %s to refs of %s", (node.node_id, node_id), {"node refs" : meta.refs}))
                meta.refs.add(node.node_id)
                logger.debug(format_message("add_node", "after %s", (node.node_id,), {"node refs" : meta.refs}))
                self.meta[node_id] = meta
        for node_id in node.subnode_depends_on:
            with self.node_lock[node_id]:
                meta = self.meta.get(node_id, NodeMetadata())
                meta.subnode_refs.add(node.node_id)
                self.meta[node_id] = meta
        if not is_hold and len(node.depends_on) == 0:
            self.ready_queue.put((node, {}, {}))

    # :param result: the result of the function, if it is Nothing then no result is returned
    def complete_node(self, node_id, ret, result):
        logger.debug(format_message("complete_node", node_id, (), {"ret": ret, "result": result}))
        
        node = self.nodes[node_id]
        meta = self.meta[node_id]
        refs = meta.refs
        subnode_refs = meta.subnode_refs
        logger.debug(format_message("complete_node", node_id, (), {"refs": refs, "subnode_refs": subnode_refs}))

        for ref in subnode_refs | refs:
            with self.node_lock[ref]:
                refmeta = self.meta[ref]
                if ref in subnode_refs:
                    if result != Nothing:
                        refmeta.subnode_results[node_id] = result.value
                    refmeta.decrement_subnode_depends()
                if ref in refs:
                    if result != Nothing:
                        refmeta.results[node_id] = result.value
                    refmeta.decrement_depends()

                if refmeta.depends == 0 and refmeta.subnode_depends == 0:
                    task = (self.nodes[ref], refmeta.results, refmeta.subnode_results)
                    self.ready_queue.put(task)
                else:
                    self.meta[ref] = refmeta

        logger.debug("complete_node: putting %s on output_queue", ret)
        self.output_queue.put(Just(ret))
            
        with self.lock:
            logger.debug("complete_node: deleting %s from self.meta", node_id)
            del self.meta[node_id]
            del self.nodes[node_id]
            logger.debug("complete_node: len(self.nodes) = %s", len(self.nodes))
            if len(self.nodes) == 0:
                self.ready_queue.put((Node(self.end_of_queue), {}, {}))
                self.output_queue.put(Nothing)

    def get_next_ready_node(self, *args, **kwargs):
        logger.debug("NodeMap.get_next_ready_node: self.ready_queue.qsize() = %s len(self.nodes) = %s", self.ready_queue.qsize(), len(self.nodes))
        node, results, subnode_results = self.ready_queue.get(*args, **kwargs)
        logger.debug("NodeMap.get_next_ready_node: node = %s self.end_of_queue = %s", node, self.end_of_queue)
        if node.o == self.end_of_queue:
            self.ready_queue.put((node, results, subnode_results))
        return node, results, subnode_results

    def get_next_output(self):
        return self.output_queue.get()

    # def empty(self):
    #     with self.lock:
    #         logger.debug("empty: len(self.nodes) == %s", len(self.nodes))
    #         return len(self.nodes) == 0

        
class DependentQueue:
    """The queue maintain a list of tasks. Before any task is added to the list, the queue is in the ready state, when the last task is compleete the queue is in the closed state. In the closed state the queue will always return end_of_queue.
    """
    def __init__(self, manager, end_of_queue):
        self.node_map = NodeMap(manager, end_of_queue)

    def put(self, o, job_id=None, ret=[], depends_on=set(), subnode_depends_on=set(), is_hold=False):
        node = Node(o, node_id=job_id, ret=ret, depends_on=depends_on, subnode_depends_on=subnode_depends_on)
        self.node_map.add_node(node, is_hold=is_hold)
        return node.node_id

    def get(self, *args, **kwargs):
        node, results, subnode_results = self.node_map.get_next_ready_node(*args, **kwargs)
        logger.debug(f"DependentQueue.get: node = %s, results = %s, subnode_results = %s", node, results, subnode_results)
        return node.get(), results, subnode_results, node.node_id
        
    def get_next_output(self):
        return self.node_map.get_next_output()
    
    def complete(self, node_id, ret, x=Nothing):
        self.node_map.complete_node(node_id, ret, x)


        
        
