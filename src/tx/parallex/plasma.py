import os
import os.path
from subprocess import Popen
from multiprocessing import Process
from dataclasses import dataclass
from uuid import uuid1
from tempfile import mkstemp

@dataclass
class PlasmaStore:
    path: str
    proc: Popen
    
DEFAULT_PLASMA_STORE_MAX_SIZE = 50000000

def start_plasma() -> PlasmaStore:
    
    fd, tmpfile = mkstemp()
    os.close(fd)
    p = Popen(["plasma_store", "-m", os.environ.get("PLASMA_STORE_MAX_SIZE", str(DEFAULT_PLASMA_STORE_MAX_SIZE)), "-s", tmpfile])
    return PlasmaStore(tmpfile, p)

def stop_plasma(p: PlasmaStore) -> None:
    p.proc.terminate()
    p.proc.wait()
    os.remove(p.path)
              
    
    
