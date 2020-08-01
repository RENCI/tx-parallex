import sys
from multiprocessing import Manager, Process
import yaml
import json
from jsonschema import validate
import os.path
import logging
from tempfile import mkstemp
import os
from tx.parallex.dependentqueue import DependentQueue, SubQueue
from tx.parallex.task import enqueue, work_on, dispatch, EndOfQueue, write_to_disk, read_from_disk
from tx.parallex.python import python_to_spec
from tx.readable_log import getLogger

logger = getLogger(__name__, logging.INFO)

with open(os.path.join(os.path.dirname(__file__), "schema.json")) as f:
    schema = json.load(f)

def run_python(number_of_workers, pyf, dataf, system_paths=[], validate_spec=True, output_path=None):
    with open(pyf) as s:
        py = s.read()
    with open(dataf) as d:
        data = yaml.safe_load(d)
    return start_python(number_of_workers, py, data, system_paths, validate_spec, output_path)


def run(number_of_workers, specf, dataf, system_paths=[], validate_spec=True, output_path=None):
    with open(specf) as s:
        spec = yaml.safe_load(s)
    with open(dataf) as d:
        data = yaml.safe_load(d)
    return start(number_of_workers, spec, data, system_paths, validate_spec, output_path)


def start_python(number_of_workers, py, data, system_paths, validate_spec, output_path):
    add_paths = list(set(system_paths) - set(sys.path))
    sys.path.extend(add_paths)
    logger.debug(f"add_paths = {add_paths}")
    try:
        spec = python_to_spec(py)
    finally:
        for _ in range(len(add_paths)):
            sys.path.pop()
    return start(number_of_workers, spec, data, system_paths, validate_spec, output_path)

                
def start(number_of_workers, spec, data, system_paths, validate_spec, output_path):
    if validate_spec:
        validate(instance=spec, schema=schema)
    if output_path is None:
        fd, temp_path = mkstemp()
        os.close(fd)
    else:
        temp_path = output_path

    try:
        with Manager() as manager:
            job_queue = DependentQueue(manager, EndOfQueue())
            enqueue(spec, data, job_queue)
            subqueues = [SubQueue(job_queue) for _ in range(number_of_workers)]
            processes = []
            for subqueue in subqueues:
                p = Process(target=work_on, args=[subqueue, system_paths])
                p.start()
                processes.append(p)
            p = Process(target=dispatch, args=[job_queue, subqueues])
            p.start()
            processes.append(p)
            p2 = Process(target=write_to_disk, args=[job_queue, temp_path])
            p2.start()
            processes.append(p2)
            for p in processes:
                p.join()
            if output_path is None:
                return read_from_disk(temp_path)

    finally:
        if output_path is None:
            os.remove(temp_path)
