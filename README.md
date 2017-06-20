# Prometheus Relation Model
*Prometheus Relation Model trains models to extract relation triples from texts.*

## Installation
This program depends on several libraries using SBT as its dependency manager
and build tool. Only one depedency (docforia) isn't publicly available through the Maven central repository.

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

### Docker
There exists a docker image that is setup to download english models based of Wikipedia. The default command starts the REST api.

```bash
docker build -t prometheus/relation_extractor .
docker run -p 8080:8080 prometheus/relation_extractor
```

It is possible to perform training and run custom commands but it requires overriding the default commands using the `docker exec` command or by passing parameters to `docker run` which will override the sbt subcommand.

Depending on your system you might need to tweak the JVM options to allow for enough memory both off-heap for Javacpp and for the JVM. Define using the `--env <key>=<val>` flag to `docker run`.
By default the image uses 14GB of ram.

## API Documentation
[ScalaDoc](https://erikgartner.github.io/prometheus-relation-model)

## Developers
- [Axel Larsson](https://github.com/AxelTLarsson)
- [Erik Gärtner](https://gartner.io)
