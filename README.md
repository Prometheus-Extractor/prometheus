# Fact Extractor

## Installation
This program depends on several libraries using SBT as its dependency manager
and build tool.

1. Install [docforia](https://github.com/marcusklang/docforia) locally using `mvn install`

## Running
There are two modes to run the program: locally and on a spark cluster.

To run locally simply use:
```
sbt run
```

To run on the cluster use:
```
./run-cluster
```

That command creates a jar containing the program and all dependencies by running `sbt -Dmode=cluster assembly` then uploads them to cluster and runs `spark-submit`.
