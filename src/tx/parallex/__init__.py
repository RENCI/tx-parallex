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

def run_python(number_of_workers, pyf, dataf, validate_spec=True):
    with open(pyf) as s:
        py = s.read()
    with open(dataf) as d:
        data = yaml.safe_load(d)
    return start_python(number_of_workers, py, data, validate_spec)


def run(number_of_workers, specf, dataf, validate_spec=True):
    with open(specf) as s:
        spec = yaml.safe_load(s)
    with open(dataf) as d:
        data = yaml.safe_load(d)
    return start(number_of_workers, spec, data, validate_spec)


def start_python(number_of_workers, py, data, validate_spec):
    return start(number_of_workers, python_to_spec(py), data, validate_spec)

def start(number_of_workers, spec, data, validate_spec):
    if validate_spec:
        validate(instance=spec, schema=schema)
    with Manager() as manager:
        job_queue = DependentQueue(manager, EndOfQueue())
        enqueue(spec, data, job_queue)
        subqueues = [SubQueue(job_queue) for _ in range(number_of_workers)]
        processes = []
        for subqueue in subqueues:
            p = Process(target=work_on, args=[subqueue])
            p.start()
            processes.append(p)
        p = Process(target=dispatch, args=[job_queue, subqueues])
        p.start()
        processes.append(p)
        for p in processes:
            p.join()
        return job_queue.get_results()


