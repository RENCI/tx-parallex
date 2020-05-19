from multiprocessing import Manager, Queue
from uuid import uuid1
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Node:
    def __init__(self, o, node_id=None, ret=None, depends_on={}):
        """
        :param o: The task object
        :param node_id: The node_id, if None, one will be generated
        :type node_id: maybe[str]
        :param depends_on: a dict where the key is node_id that it depends on and the value is an iterable that returns a list of keys. The keys are used to pass the return value of the nodes to this node.
        :type depends_on: maybe[dict[str, iterable[str]]]
        """
        self.o = o
        self.node_id = node_id if node_id is not None else str(uuid1())
        self.ret = ret
        self.depends_on = depends_on

    def get(self):
        return self.o


class NodeMap:
    """
    :attr refs: a map from node_id to node_ids that depends on it
    :type refs: dict[str, str]
    :attr depends: a map from node_id to a dict where the key is node_id that it depends on and the value is an iterable that returns a list of keys
    :type depends: dict[str, iterable[str]]
    :attr nodes:
    :type nodes: dict[str, Node]
    :attr results: a map from node_id to partial or complete results of the nodes that it depends on
    :type results: dict[str, dict[str, any]]
    :attr results: a map from node_id to results of the node, for nodes with no dependencies. These object are store in memory. To ignore the result, set dependencies to empty iterable.
    :type results: dict[str, dict[str, any]]
    :attr ready_queue: a queue of tasks that are ready
    :type ready_queue: Queue[Node]
    :attr lock: global lock
    :type lock: Lock
    """
    
    def __init__(self, manager):
        self.refs = manager.dict()
        self.depends = manager.dict()
        self.nodes = manager.dict()
        self.results = manager.dict()
        self.ready_queue = manager.Queue()
        self.lock = manager.Lock()
        self.outputs = manager.dict()

    def add_node(self, node):
        with self.lock:
            if node.node_id in self.nodes:
                raise RuntimeError(f"{node.node_id} is already in the map")
            
            self.nodes[node.node_id] = node
            self.depends[node.node_id] = node.depends_on
            logger.info(f"add_node: {node.node_id} depends_on {node.depends_on}")
            for node_id in node.depends_on.keys():
                self.refs[node_id] = self.refs.get(node_id, []) + [node.node_id]
            if len(node.depends_on) == 0:
                self.ready_queue.put((node, {}))

    def complete_node(self, node_id, result):
        with self.lock:
            logger.info(f"complete_node: {node_id} complete")
            node = self.nodes[node_id]
            ret_names = node.ret
            refs = self.refs.get(node_id)
            logger.info(f"complete_node: refs = {refs}")
            if refs is not None:
                for ref in refs:
                    refdep = dict(self.depends[ref])
                    ks = refdep[node_id]
                    del refdep[node_id]
                    self.depends[ref] = refdep
                    refresults = {**self.results.get(ref, {}), **{k: result for k in ks}}
                    logger.info(f"complete_node: ref = {ref}, refresults = {refresults}")
                    self.results[ref] = refresults
                    if len(refdep) == 0:
                        task = (self.nodes[ref], refresults)
                        logger.info(f"complete_node: ref = {ref}, len(refdep) == 0, task = {task}")
                        self.ready_queue.put(task)
                        del self.depends[ref]
                        del self.results[ref]
                del self.refs[node_id]
            for ret_name in ret_names:
                # terminal node
                self.outputs[ret_name] = result
            del self.nodes[node_id]

    def get_next_ready_node(self, *args, **kwargs):
        return self.ready_queue.get(*args, **kwargs)

        
class DependentQueue:
    def __init__(self, manager):
        self.node_map = NodeMap(manager)

    def put(self, o, job_id=None, ret=[], depends_on={}):
        node = Node(o, node_id=job_id, ret=ret, depends_on=depends_on)
        self.node_map.add_node(node)
        return node.node_id

    def get(self, *args, **kwargs):
        node, result = self.node_map.get_next_ready_node(*args, **kwargs)
        logger.info(f"DependentQueue.get: node = {node}, result = {result}")
        return node.get(), result, node.node_id

    def complete(self, node_id, x=None):
        self.node_map.complete_node(node_id, x)

    def get_results(self):
        return dict(self.node_map.outputs)


class SubQueue:
    def __init__(self, queue):
        self.queue = queue
        self.subqueue = Queue()

    def put(self, o):
        self.subqueue.put(o)

    def get(self, *args, **kwargs):
        return self.subqueue.get(*args, **kwargs)

    def complete(self, node_id, x=None):
        self.queue.complete(node_id, x)

    def full(self):
        return self.subqueue.full()
        
        
