import json

from inspector.writer import Writer
from pyarrow.fs import FileSystem


class FileWriter(Writer):
    def __init__(self, fs: FileSystem):
        self.fs = fs

    def write(self, metrics, uri: str):
        with self.fs.open_output_stream(uri) as stream:
            stream.write(json.dumps(metrics).encode('utf-8'))
