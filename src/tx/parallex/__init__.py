import sys
from multiprocessing import Manager, Process
from tx.parallex.dependentqueue import DependentQueue, SubQueue
from tx.parallex.task import enqueue, work_on, dispatch, EndOfQueue
from tx.parallex.python import python_to_spec
import yaml
import json
from jsonschema import validate
import os.path

with open(os.path.join(os.path.dirname(__file__), "schema.json")) as f:
    schema = json.load(f)

def run_python(number_of_workers, pyf, dataf, system_paths=[], validate_spec=True):
    with open(pyf) as s:
        py = s.read()
    with open(dataf) as d:
        data = yaml.safe_load(d)
    return start_python(number_of_workers, py, data, system_paths, validate_spec)


def run(number_of_workers, specf, dataf, system_paths=[], validate_spec=True):
    with open(specf) as s:
        spec = yaml.safe_load(s)
    with open(dataf) as d:
        data = yaml.safe_load(d)
    return start(number_of_workers, spec, data, system_paths, validate_spec)


def start_python(number_of_workers, py, data, system_paths, validate_spec):
    return start(number_of_workers, python_to_spec(py), data, system_paths, validate_spec)

def start(number_of_workers, spec, data, system_paths, validate_spec):
    if validate_spec:
        validate(instance=spec, schema=schema)
    with Manager() as manager:
        job_queue = DependentQueue(manager, EndOfQueue())
        add_paths = list(set(system_paths) - set(sys.path))
        sys.path.extend(add_paths)
        try:
            enqueue(spec, data, job_queue)
        finally:
            for _ in range(len(add_paths)):
                sys.path.pop()
        subqueues = [SubQueue(job_queue) for _ in range(number_of_workers)]
        processes = []
        for subqueue in subqueues:
            p = Process(target=work_on, args=[subqueue, system_paths])
            p.start()
            processes.append(p)
        p = Process(target=dispatch, args=[job_queue, subqueues])
        p.start()
        processes.append(p)
        for p in processes:
            p.join()
        return job_queue.get_results()



