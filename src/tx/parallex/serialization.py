from tx.functional.either import Left, Right
from tx.parallex.data import Starred

def jsonify(o):
    if isinstance(o, Left):
        return {
            "left": jsonify(o.value)
        }
    elif isinstance(o, Right):
        return {
            "right": jsonify(o.value)
        }
    elif isinstance(o, Starred):
        return {
            "starred": jsonify(o.value)
        }
    elif isinstance(o, range):
        return {
            "range": {
                "start": jsonify(o.start),
                "stop": jsonify(o.stop),
                "step": jsonify(o.step)
            }
        }
    elif isinstance(o, dict) and ("left" in o or "right" in o or "starred" in o or "range" in o or "json" in o):
        return {
            "json": o
        }
    else:
        return o

def unjsonify(o):
    if isinstance(o, dict) and "left" in o:
        return Left(unjsonify(o["left"]))
    elif isinstance(o, dict) and "right" in o:
        return Right(unjsonify(o["right"]))
    elif isinstance(o, dict) and "starred" in o:
        return Starred(unjsonify(o["starred"]))
    elif isinstance(o, dict) and "range" in o:
        return range(unjsonify(o["range"]["start"]), unjsonify(o["range"]["stop"]), unjsonify(o["range"]["step"]))
    elif isinstance(o, dict) and "json" in o:
        return o["json"]
    else:
        return o


