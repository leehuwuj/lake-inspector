from inspector.file_backend import FileBackend


class HdfsFileBackend(FileBackend):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
