from terminaltables import AsciiTable
from textwrap import wrap
import logging
import os

def getLogger(name, level):
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.propagate = 0
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(process)d - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(os.environ.get("LOG_LEVEL", level))
        print(f"adding handler to logger {name}")
    return logger

def wrap_line(s):
    return "\n".join(wrap(s, 80))


class format_message:
    def __init__(self, title, description, obj):
        self.title = title
        self.description = description
        self.obj = obj
        
    def __str__(self):
        table_data = [
            ["message", wrap_line(self.description)]
        ] + [[wrap_line(str(k)), wrap_line(str(v))] for k, v in self.obj.items()]
        
        table = AsciiTable(table_data, self.title)
        return f"\n{table.table}"
