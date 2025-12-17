import os


def generate_local_path(path: str):
    return os.path.join("odw/test/resources/datalake", path)
