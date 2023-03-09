import logging

from inspector.file_backend.local import LocalFileBackend

logger = logging.getLogger(__name__)


class PartitionAnalyzer:
    def __init__(self, backend: LocalFileBackend):
        self.backend = backend

    def analyze(self, path):
        partition_size = self.backend.get_size(path)
        partition_file_count = self.backend.count_partition_files(path)
        f1_partitions = [p.path for p in self.backend.get_all_f1_partitions(path)]
        try:
            latest_partition = self.backend.get_latest_partitions(path)
        except Exception as e:
            logger.error(e)
            latest_partition = None
        return {
            'path': path,
            'size': partition_size,
            'number_of_files': partition_file_count,
            'f1_partitions': f1_partitions,
            'latest_partition': latest_partition
        }
