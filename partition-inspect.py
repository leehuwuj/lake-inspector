import os
import click
import pyarrow

from pyarrow import fs

from inspector.file_backend.hdfs import HdfsFileBackend
from inspector.partition_analyzer import PartitionAnalyzer
from inspector.writer.console import ConsoleWriter
from inspector.writer.file import FileWriter

HDFS_USER = os.environ.get("HDFS_USER", None)
HDFS_KRB_TICKET_CACHE = os.environ.get("HDFS_KRB_TICKET_CACHE", None)


@click.command()
@click.argument('path', type=str)
@click.option('--writer',
              type=click.Choice(
                  ['console', 'hdfs', 'file'],
                  case_sensitive=False
              ),
              default='console')
@click.option('--write-uri',
              type=str,
              default=None)
def analyze(path: str, writer, write_uri):
    hdfs = pyarrow.fs.HadoopFileSystem(
        host='nameservice1',
        user=HDFS_USER,
        kerb_ticket=HDFS_KRB_TICKET_CACHE
    )
    file_be = HdfsFileBackend(
        client=hdfs
    )
    partition_analyzer = PartitionAnalyzer(
        backend=file_be
    )
    metrics = partition_analyzer.analyze(
        path=path
    )
    if writer == 'console':
        writer = ConsoleWriter()
        writer.write(metrics, uri=None)
    elif writer == 'hdfs':
        if write_uri is None:
            raise Exception("Missing --write-uri argument!")
        writer = FileWriter(fs=hdfs)
        writer.write(metrics, uri=write_uri)
    elif writer == 'file':
        if write_uri is None:
            raise Exception("Missing --write-uri argument!")
        local_file = fs.LocalFileSystem()
        writer = FileWriter(fs=local_file)
        writer.write(metrics, uri=write_uri)


if __name__ == '__main__':
    analyze()
