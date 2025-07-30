from setuptools import setup, find_packages

root_package = "odw"

setup(
    name="odw",
    version="1",
    packages=[root_package] + [f"{root_package}.{item}" for item in find_packages(where=root_package) if "test" not in item]
)
