import os
import click
import pyarrow
import pyarrow.fs

from inspector.file_backend.s3 import S3FileBackend
from inspector.partition_analyzer import PartitionAnalyzer
from inspector.writer.console import ConsoleWriter
from inspector.writer.file import FileWriter

HDFS_USER = os.environ.get("HDFS_USER", None)
HDFS_KRB_TICKET_CACHE = os.environ.get("HDFS_KRB_TICKET_CACHE", None)
HDFS_HOST = os.environ.get("HDFS_USER", None)


@click.command()
@click.argument('path', type=str)
@click.option('--writer',
              type=click.Choice(
                  ['console', 's3', 'file'],
                  case_sensitive=False
              ),
              default='console')
@click.option('--write-uri',
              type=str,
              default=None)
def analyze(path: str, writer, write_uri):
    storage_options = {
        'access_key': os.environ.get('AWS_ACCESS_KEY_ID', None),
        'secret_key': os.environ.get('AWS_SECRET_ACCESS_KEY', None),
        'endpoint_override': os.environ.get('AWS_ENDPOINT_URL', None),
    }
    arrow_s3 = pyarrow.fs.S3FileSystem(**storage_options)
    file_be = S3FileBackend(client=arrow_s3)
    partition_analyzer = PartitionAnalyzer(
        backend=file_be
    )
    metrics = partition_analyzer.analyze(
        path=path
    )
    if writer == 'console':
        writer = ConsoleWriter()
        writer.write(metrics, uri=None)
    elif writer == 's3':
        if write_uri is None:
            raise Exception("Missing --write-uri argument!")
        writer = FileWriter(fs=arrow_s3)
        writer.write(metrics, uri=write_uri)
    elif writer == 'file':
        if write_uri is None:
            raise Exception("Missing --write-uri argument!")
        local_file = pyarrow.fs.LocalFileSystem()
        writer = FileWriter(fs=local_file)
        writer.write(metrics, uri=write_uri)


if __name__ == '__main__':
    analyze()
