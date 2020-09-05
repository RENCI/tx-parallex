import sys
import logging
from tx.functional.either import Left, Right, Either
from tx.functional.maybe import Just, Nothing
from tx.readable_log import format_message, getLogger
from typing import List, Any, Dict, Tuple, Set, Callable, TypeVar, TextIO
from .dependentqueue import DependentQueue
from .task import EndOfQueue

logger = getLogger(__name__, logging.INFO)

def work_on(queue : DependentQueue, output_path: str, library_paths : List[str]) -> None:
    with open(output_path, "w") as output:
        logger.debug("library_paths = %s", library_paths)
        sys.path.extend(library_paths)
        queue.init_thread()
        while True:
            jri = queue.get()
            job, results, subnode_results, jid = jri
            if isinstance(job, EndOfQueue):
                break
            else:
                logger.debug(format_message("work_on", "task begin.", {
                    "job": job,
                    "jid": jid,
                    "params": results
                }))
                logger.info("task begin %s", jid)
                resultj = job.run(results, subnode_results, queue, output)
                logger.debug(format_message("work_on", "task complete.", {
                    "job": job,
                    "jid": jid,
                    "resultj": resultj,
                    "params": results
                }))
                logger.info(f"task finish %s", jid)
                queue.complete(jid, resultj)
    


    

    

        
        
    
