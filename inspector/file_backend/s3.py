from inspector.file_backend import FileBackend


class S3FileBackend(FileBackend):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
