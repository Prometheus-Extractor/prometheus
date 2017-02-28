# Prometheus Relation Model
*Prometheus Relation Model trains models to extract relation triples from texts.*

## Installation
This program depends on several libraries using SBT as its dependency manager
and build tool.

1. Install [docforia](https://github.com/marcusklang/docforia) by cloning and then `mvn install`
2. Run `sbt compile`

## Running
There are two modes to run the program: locally and on a spark cluster.

To run locally simply use:
```
sbt "run /path/to/herd/annotated /path/to/extracted/entities"
```

To run on the cluster use:
```
./run-cluster.sh "hdfs:/path/to/herd/annotated/corpus" "hdfs:/path/to/extracted/entities"
```

That command creates a jar containing the program and all dependencies by running `sbt -Dmode=cluster assembly` then uploads it to cluster and runs `spark-submit`.

## API Documentation
[ScalaDoc](https://erikgartner.github.io/prometheus-relation-model)

## Developers
- [Axel Larsson](https://github.com/AxelTLarsson)
- [Erik Gärtner](https://gartner.io)
