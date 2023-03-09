from inspector.file_backend import FileBackend


class LocalFileBackend(FileBackend):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
