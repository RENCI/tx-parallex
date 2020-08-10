from terminaltables import AsciiTable
from textwrap import wrap
import logging
import os

def getLogger(name, level):
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.propagate = 0
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(process)d - %(threadName)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(os.environ.get("LOG_LEVEL", level))
    return logger

def wrap_line(s):
    return "\n".join(wrap(s, 80))


def to_val(a):
    return a() if callable(a) else a

class format_message:
    def __init__(self, title, description, obj):
        self.title = title
        self.description = description
        self.obj = obj
        
    def __str__(self):
        table_data = [
            ["message", wrap_line(str(to_val(self.description)))]
        ] + [[wrap_line(str(k)), wrap_line(str(to_val(v)))] for k, v in to_val(self.obj).items()]
        
        table = AsciiTable(table_data, str(to_val(self.title)))
        return f"\n{table.table}"
