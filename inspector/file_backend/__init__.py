import os
import re
from typing import List, Any

import pyarrow as pa
from pyarrow import fs
from pyarrow.fs import FileSystem

from inspector.exceptions import CheckingPartitionException

HIVE_PARTITION_REGEX = r"[a-zA-Z0-9_]*.=.[a-zA-Z0-9-]*"


class HivePartition:
    @staticmethod
    def match(path: str):
        """
        Check data is path a partition
        by using regex
        """
        base_dir = os.path.basename(path)
        return bool(re.match(HIVE_PARTITION_REGEX, base_dir))

    @staticmethod
    def extract_value(path: str):
        tail = path.split('/')[-1]
        return tail.split('=')


class FileBackend:
    def __init__(self, client: FileSystem) -> None:
        self.client = client

    def _get_file_info(self, path, recursive=False):
        file_selector = pa.fs.FileSelector(path, recursive=recursive)
        return self.client.get_file_info(file_selector)

    def get_all_partitions(self, path) -> List[Any]:
        """
        Get all partitions (1st level) in a path
        :param path:
        :return:
        """
        list_info = self._get_file_info(path, recursive=True)
        if len(list_info) == 0:
            raise CheckingPartitionException("There is no partition in the"
                                             "path: {}".format(path))
        else:
            directory = [p for p in list_info if not p.is_file]
            parts = [p for p in directory if HivePartition.match(p.path)]
            return parts

    def get_all_f1_partitions(self, path) -> List[Any]:
        list_info = self._get_file_info(path)
        if len(list_info) == 0:
            raise CheckingPartitionException("There is no partition in the"
                                             "path: {}".format(path))
        else:
            directory = [p for p in list_info if not p.is_file]
            parts = [p for p in directory if HivePartition.match(p.path)]
            return parts

    def get_latest_partitions(self, path):
        all_partitions = self.get_all_partitions(path=path)
        partitions = [HivePartition.extract_value(part.path) + [part.path]
                      for part in all_partitions]
        # Assume that latest partition is the one has max partition value
        return max(partitions, key=lambda item: item[2])

    def count_partition_files(self, path):
        list_f_info = self._get_file_info(path, recursive=True)
        file_only = [f for f in list_f_info if f.is_file]
        return len(file_only)

    def get_size(self, path):
        f_info = self._get_file_info(path, recursive=True)
        total_size = 0
        for f in f_info:
            if f.is_file:
                total_size += f.size
        return total_size
