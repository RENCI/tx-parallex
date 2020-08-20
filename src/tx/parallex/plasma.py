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
    
def start_plasma(mem_size: int) -> PlasmaStore:
    
    fd, tmpfile = mkstemp()
    os.close(fd)
    p = Popen(["plasma_store", "-m", str(mem_size), "-s", tmpfile])
    return PlasmaStore(tmpfile, p)

def stop_plasma(p: PlasmaStore) -> None:
    p.proc.terminate()
    p.proc.wait()
    os.remove(p.path)
              
    
    
