from pprint import pprint
from inspector.writer import Writer


class ConsoleWriter(Writer):
    def write(self, metrics, uri: str):
        pprint(metrics)
