import logging
import pyarrow.plasma as plasma
from tx.readable_log import getLogger, format_message
from .serialization import jsonify, unjsonify


logger = getLogger(__name__, logging.INFO)


class ObjectStore:
    def __init__(self, manager, plasma_store):
        self.manager = manager
        self.shared_ref_dict = manager.dict()
        self.shared_ref_lock_dict = manager.dict()
        self.plasma_store = plasma_store


    def init_thread(self):
        self.client = plasma.connect(self.plasma_store)
        
    def put(self, o) :    
        oid = self.client.put(jsonify(o))
        logger.debug(format_message("ObjectStore.put", "putting object into shared memory store", {"o": o, "oid": oid}))
        self.shared_ref_lock_dict[oid] = self.manager.Lock()
        self.shared_ref_dict[oid] = 0
        return oid

    def increment_ref(self, oid):
        with self.shared_ref_lock_dict[oid]:
            val = self.shared_ref_dict[oid]
            val += 1
            self.shared_ref_dict[oid] = val
            logger.debug(format_message("ObjectStore.increment_ref", "incrementing object ref count", {"oid": oid, "val": val}))
        
    def decrement_ref(self, oid):
        with self.shared_ref_lock_dict[oid]:
            val = self.shared_ref_dict[oid]
            val -= 1
            logger.debug(format_message("ObjectStore.decrement_ref", "decrement object ref count", {"oid": oid, "val": val}))
            
            if val == 0:
                logger.debug(format_message("ObjectStore.decrement_ref", "deleting object", {"oid": oid}))
                self.client.delete([oid])
                del self.shared_ref_dict[oid]
                del self.shared_ref_lock_dict[oid]
            else:
                self.shared_ref_dict[oid] = val
                
    def get(self, oid):
        logger.debug(format_message("ObjectStore.get", "getting object from shared memory store", {"oid": oid}))
        return unjsonify(self.client.get(oid))

    
