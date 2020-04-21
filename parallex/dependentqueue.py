from multiprocessing import Manager
from uuid import uuid1


class Node:
    def __init__(self, o, node_id=None, depends_on={}):
        self.o = o
        self.node_id = node_id if node_id is not None else str(uuid1())
        self.depends_on = depends_on

    def get(self):
        return self.o


def get_keys_by_value(d, vs):
    ks = []
    for k, v in d.items():
        if v == vs:
            ks += [k]
    return ks


class NodeMap:
    def __init__(self, manager):
        self.refs = manager.dict()
        self.depends = manager.dict()
        self.nodes = manager.dict()
        self.results = manager.dict()
        self.ready_queue = manager.Queue()
        self.lock = manager.Lock()

    def add_node(self, node):
        with self.lock:
            if node.node_id in self.nodes:
                raise RuntimeError(f"{node.node_id} is already in the map")
            
            self.nodes[node.node_id] = node
            self.depends[node.node_id] = node.depends_on
            print(f"add {node.node_id} {node.depends_on}")
            for node_id in node.depends_on.values():
                self.refs[node_id] = self.refs.get(node_id, []) + [node.node_id]
            if len(node.depends_on) == 0:
                self.ready_queue.put((node, {}))

    def complete_node(self, node_id, result):
        with self.lock:
            print(f"{node_id} complete")
            node = self.nodes[node_id]
            refs = self.refs.get(node_id)
            print(f"refs = {refs}")
            if refs is not None:
                for ref in refs:
                    refdep = self.depends[ref]
                    ks = get_keys_by_value(refdep, node_id)
                    refdep = {k : v for k, v in refdep.items() if k not in ks}
                    self.depends[ref] = refdep
                    self.results[ref] = {**self.results.get(ref, {}), **{k: result for k in ks}}
                    if len(refdep) == 0:
                        self.ready_queue.put((self.nodes[ref], self.results[ref]))
                        del self.depends[ref]
                        del self.results[ref]
                del self.refs[node_id]

            del self.nodes[node_id]

    def get_next_ready_node(self, *args, **kwargs):
        return self.ready_queue.get(*args, **kwargs)

        
class DependentQueue:
    def __init__(self, ready_queue):
        self.node_map = NodeMap(ready_queue)

    def put(self, o, depends_on={}):
        node = Node(o, depends_on=depends_on)
        self.node_map.add_node(node)
        return node.node_id

    def get(self, *args, **kwargs):
        node, result = self.node_map.get_next_ready_node(*args, **kwargs)
        return node.get(), result, lambda x : self.node_map.complete_node(node.node_id, x)
        
        
