import sys
import yappi
from tx.parallex import run_python

nthreads = int(sys.argv[1])
clock_type = sys.argv[2]

yappi.set_clock_type(clock_type)
yappi.start()

run_python(nthreads, "profile/spec.py", "profile/data.yaml")

yappi.stop()

stats = yappi.get_func_stats()
stats.sort("tsub")
stats.print_all()
