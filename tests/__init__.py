import pyarrow as pa
import pytest


@pytest.fixture
def local_file_client():
    return pa.fs.LocalFileSystem()
