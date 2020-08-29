import logging
from tx.readable_log import getLogger, format_message
from .serialization import jsonify, unjsonify
from abc import ABC, abstractmethod
from multiprocessing import Manager
from uuid import uuid1
from typing import Any
try:
    import pyarrow.plasma as plasma
    from .plasma import start_plasma, stop_plasma
except ModuleNotFoundError as e:
    pass
    

logger = getLogger(__name__, logging.INFO)


class ObjectStore(ABC):
    @abstractmethod
    def init_thread(self) -> None:
        pass

    @abstractmethod
    def init(self) -> None:
        pass

    @abstractmethod
    def shutdown(self) -> None:
        pass
        
    @abstractmethod
    def put(self, o: Any) -> str :
        pass

    @abstractmethod
    def increment_ref(self, oid: str) -> None:
        pass
        
    @abstractmethod
    def decrement_ref(self, oid: str) -> None:
        pass
                
    @abstractmethod
    def get(self, oid: str) -> Any:
        pass


class PlasmaStore(ObjectStore):
    def __init__(self, manager: Manager, mem_size: int):
        self.manager = manager
        self.shared_ref_dict = manager.dict()
        self.shared_ref_lock_dict = manager.dict()
        self.mem_size = mem_size

    def init_thread(self) -> None:
        self.client = plasma.connect(self.plasma_store.path)
        
    def init(self) -> None:
        self.plasma_store = start_plasma(self.mem_size)

    def shutdown(self) -> None:
        stop_plasma(self.plasma_store)
        
    def put(self, o: Any) -> str :    
        oid = self.client.put(jsonify(o))
        logger.debug(format_message("PlasmaStore.put", "putting object into shared memory store", {"o": o, "oid": oid}))
        self.shared_ref_lock_dict[oid] = self.manager.Lock()
        self.shared_ref_dict[oid] = 0
        return oid

    def increment_ref(self, oid: str) -> None:
        with self.shared_ref_lock_dict[oid]:
            val = self.shared_ref_dict[oid]
            val += 1
            self.shared_ref_dict[oid] = val
            logger.debug(format_message("PlasmaStore.increment_ref", "incrementing object ref count", {"oid": oid, "val": val}))
        
    def decrement_ref(self, oid: str) -> None:
        with self.shared_ref_lock_dict[oid]:
            val = self.shared_ref_dict[oid]
            val -= 1
            logger.debug(format_message("PlamsaStore.decrement_ref", "decrement object ref count", {"oid": oid, "val": val}))
            
            if val == 0:
                logger.debug(format_message("PlasmaStore.decrement_ref", "deleting object", {"oid": oid}))
                self.client.delete([oid])
                del self.shared_ref_dict[oid]
                del self.shared_ref_lock_dict[oid]
            else:
                self.shared_ref_dict[oid] = val
                
    def get(self, oid: str) -> Any:
        logger.debug(format_message("PlasmaStore.get", "getting object from shared memory store", {"oid": oid}))
        return unjsonify(self.client.get(oid))

    
class SimpleStore(ObjectStore):
    def __init__(self, manager: Manager):
        self.manager = manager
        self.shared_ref_dict = manager.dict()
        self.shared_ref_lock_dict = manager.dict()
        self.store = manager.dict()

    def init_thread(self):
        pass

    def init(self):
        pass

    def shutdown(self):
        pass
        
    def put(self, o: Any) -> str :    
        oid = str(uuid1())
        self.store[oid] = o
        logger.debug(format_message("SimpleStore.put", "putting object into shared memory store", {"o": o, "oid": oid}))
        self.shared_ref_lock_dict[oid] = self.manager.Lock()
        self.shared_ref_dict[oid] = 0
        return oid

    def increment_ref(self, oid: str) -> None:
        with self.shared_ref_lock_dict[oid]:
            val = self.shared_ref_dict[oid]
            val += 1
            self.shared_ref_dict[oid] = val
            logger.debug(format_message("SimpleStore.increment_ref", "incrementing object ref count", {"oid": oid, "val": val}))
        
    def decrement_ref(self, oid: str) -> None:
        with self.shared_ref_lock_dict[oid]:
            val = self.shared_ref_dict[oid]
            val -= 1
            logger.debug(format_message("SimpleStore.decrement_ref", "decrement object ref count", {"oid": oid, "val": val}))
            
            if val == 0:
                logger.debug(format_message("SimpleStore.decrement_ref", "deleting object", {"oid": oid}))
                del self.store[oid]
                del self.shared_ref_dict[oid]
                del self.shared_ref_lock_dict[oid]
            else:
                self.shared_ref_dict[oid] = val
                
    def get(self, oid: str) -> Any:
        logger.debug(format_message("SimpleStore.get", "getting object from shared memory store", {"oid": oid}))
        return self.store[oid]

    
