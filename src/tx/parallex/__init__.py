import sys
from multiprocessing import Manager, Process
import yaml
import json
from jsonschema import validate
import os.path
import logging
from tempfile import mkstemp, mkdtemp
import os
import shutil
from .dependentqueue import DependentQueue
from .task import enqueue, EndOfQueue, either_data
from .process import work_on
from .io import read_from_disk, merge_files
from .python import python_to_spec
from .spec import dict_to_spec
from .objectstore import PlasmaStore, SimpleStore
from tx.readable_log import getLogger

logger = getLogger(__name__, logging.INFO)

with open(os.path.join(os.path.dirname(__file__), "schema.json")) as f:
    schema = json.load(f)

def run_python(number_of_workers, pyf, dataf, system_paths=[], validate_spec=True, output_path=None, level=0, object_store=None):
    with open(pyf) as s:
        py = s.read()
    with open(dataf) as d:
        data = yaml.safe_load(d)
    return start_python(number_of_workers, py, data, system_paths, validate_spec, output_path, level, object_store)


def run(number_of_workers, specf, dataf, system_paths=[], validate_spec=True, output_path=None, level=0, object_store=None):
    with open(specf) as s:
        spec = yaml.safe_load(s)
    with open(dataf) as d:
        data = yaml.safe_load(d)
    return start(number_of_workers, spec, data, system_paths, validate_spec, output_path, level, object_store)


def start_python(number_of_workers, py, data, system_paths, validate_spec, output_path, level, object_store):
    add_paths = list(set(system_paths) - set(sys.path))
    sys.path.extend(add_paths)
    logger.debug(f"add_paths = {add_paths}")
    try:
        spec = python_to_spec(py)
    finally:
        for _ in range(len(add_paths)):
            sys.path.pop()
    return start(number_of_workers, spec, data, system_paths, validate_spec, output_path, level, object_store)


DEFAULT_PLASMA_STORE_SIZE = 50000000


def start(number_of_workers, spec, data, system_paths, validate_spec, output_path, level, object_store):
    if validate_spec:
        validate(instance=spec, schema=schema)
    if output_path is None:
        temp_dir = mkdtemp()
    else:
        with open(output_path, "w"):
            pass
        output_dir = os.path.dirname(output_path)
        temp_dir = mkdtemp(dir=output_dir)
        
    shutdown_object_store = False
    temp_path = None
    
    try:

        with Manager() as manager:
            if object_store is None:
                try:
                    logger.info("using PlasmaStore")
                    object_store = PlasmaStore(manager, DEFAULT_PLASMA_STORE_SIZE)
                except:
                    logger.info("using SimpleStore")
                    object_store = SimpleStore(manager)
                object_store.init()
                shutdown_object_store = True
            
            job_queue = DependentQueue(manager, EndOfQueue(), object_store)
            enqueue(dict_to_spec(spec), either_data(data), job_queue, level=level)
            processes = []
            output_paths = []
            for _ in range(number_of_workers):
                fd, path = mkstemp(dir=temp_dir)
                os.close(fd)
                output_paths.append(path)
                p = Process(target=work_on, args=(job_queue, path, system_paths))
                p.start()
                processes.append(p)
                
            for p in processes:
                p.join()

            if output_path is None:
                fd, temp_path = mkstemp(dir=temp_dir)
                os.close(fd)
            else:
                temp_path = output_path

            merge_files(output_paths, temp_path)
                
            if output_path is None:
                return read_from_disk(temp_path)
            else:
                return None

    finally:
        if shutdown_object_store:
            object_store.shutdown()
        shutil.rmtree(temp_dir)
