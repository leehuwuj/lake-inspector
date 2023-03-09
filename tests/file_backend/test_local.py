import pytest
import pyarrow.fs
import pyarrow as pa
from inspector.file_backend.local import LocalFileBackend


@pytest.fixture
def local_file_client():
    return pa.fs.LocalFileSystem()


class TestLocalFileBackend:

    def test_get_all_partitions(self, local_file_client):
        local_fb = LocalFileBackend(
            client=local_file_client
        )
        res = local_fb.get_all_partitions(
            path="tests/example_data"
        )
        assert len(res) == 2

    def test_get_latest_partitions(self, local_file_client):
        local_fb = LocalFileBackend(
            client=local_file_client
        )
        res = local_fb.get_latest_partitions(
            path="tests/example_data"
        )
        assert res == '20230102'

    def test_count_partition_files(self, local_file_client):
        local_fb = LocalFileBackend(
            client=local_file_client
        )
        res = local_fb.count_partition_files(
            path="tests/example_data"
        )
        assert res == 1

    def test_get_size(self, local_file_client):
        local_fb = LocalFileBackend(
            client=local_file_client
        )
        res = local_fb.get_size(
            path="tests/example_data"
        )
        assert res == 0