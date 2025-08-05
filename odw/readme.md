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
│    └── pyproject.toml # Where the build config is defined
```


## Deployment
The ODW package is built into a `.whl` file, which is uploaded to the Synapse workspace. The wheel file name contains the commit hash of the last commit that modified the `./odw` directory, which is used to identify which version of the wheel has been deployed to the workspace (unfortunately it is not possible to download the wheel after it has been uploaded).

Only 1 ODW package is active at a time. When a new package is deployed, the new package is installed, then old package is uninstalled. If there are two or more ODW packages detected during deployment, then it is very likely that there are concurrent deployments, and the most recent deployment is aborted (Note, there is an edge case that can cause a deadlock if both concurrent builds reach the workspace upload step at the exact same time. However in practice this is unlikely due to the time it takes to spin up an ADO deployment)
