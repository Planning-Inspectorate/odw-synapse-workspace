# ODW Python package
This folder holds the definition of the ODW Python package. Complex notebook functionality has been defined in this package so that we can efficiently perform testing without needing to run hundreds of notebooks (which will take a long time to execute). This package is installed on the spark pools, which allows Synapse to use anything we define here

## Directory structure

```
├── odw
│    └── core/ # Define ETL functinonality to be used in Synapse
│    └── test/
│    │   └── unit_test/ # Where unit tests are defined
│    │   └── integration_test/ # Where integration tests are defined
│    │   └── resources/ # Where any resources used by the tests are defined
│    └── build-requirements.txt # Where the requirements for package building are defined
│    └── pyproject.toml # Where the build config is defined
```
