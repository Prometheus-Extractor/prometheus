# Prometheus Relation Model
*Prometheus Relation Model trains models to extract relation triples from texts.*

## Design
The Prometheus program is built around the principle of stages. Each stage is responsible for one step
in the process of extracting training data, training models, extracting relations and evaluation.

Each step caches it's result in the work directory and only generates the data if it is needed by a following step.
Using the `--stage <stage>`-flag you can limit the execution up to a certain stage (and all of it's dependencies).

Current design uses two models, one fast for filtering and then one precise for classification.

### Stages
Below follows a short overview of the stages.

#### Corpus Reader
*(Annotated Corpus Folder -> CorpusData)*

Reads the annotated corpus from disk to memory.

#### Entity Pair Extractor
*(Config file, Wikidata -> EntityPairs)*

Extracts all entities pair that uses that are connected by relation defined in the configuration file.

#### Training Data Extractor
*(EntityPairs, Annotated Corpus -> TrainingSentences)*

Extracts all sentences in the corpus containing any entity pair.

#### PoS/Dep/NEType Encoders
*(Annotated Corpus -> PoS/Dep/NEType Encoders)*

The encoders learns numeric representations for the features. The Word2Vec encoder is special since it uses an externally trained Word2Vec-model that has to be supplied as a program argument.

#### Feature Extractor
*(TrainingSentences -> FeatureArrays)*

Creates (string) feature arrays by extracting the features for the TrainingSentences.

#### Feature Transformer
*(FeatureArrays -> VectorFeatures)* `--stage preprocess`

Transforms the string features to numeric features using the encoders. Since Prometheus use Word2Vec the features are stored as dense vectors that are quite space consuming.

#### Filter Model
*(VectorFeatures -> FilterModel)* `--stage train`

Trains the first model that performs filtering of between relevant/irrelevant sentences.

#### Classification Model
*(VectorFeatures -> ClassificationModel)* `--stage train`

Trains the second model that performs classification of relevant sentences between the classes / relations defined in the configuration file.

#### Predictor
*(CorpusData, Models, Encoders -> Extractions)* `--stage full`

This stage runs the models over the entire CorpusData to extract all relations found in the text. Note that this CorpusData can but doesn't have to be the same as the the one used during Training Data Extraction.

#### Model Evaluation
*(Models, EvaluationFiles -> EvaluationResults)* `--model-evaluation-files <files>`

Using labeled evaluation sentences (found in `data/model_evaluation`) the model performance is evaluated. Uses the external annotation server to annotate the evaluation sentences on-the-fly.

#### Data Evaluation
*(Models, Extractions, Wikidata -> EvaluationResults)* `--data-evaluation>`

Compares the Extractions against the fact found in Wikidata to evaluate the number of correct/incorrect extractions.

#### Demo
*(Models, Encoders -> REST API)* `-d`

Technically not a stage, this command serves a simple REST API at `0.0.0.0:8080/api/<lang>/extract`. When sending a text in the body of a POST request it gets annotated by the external annotation server and then the system replies with extracted relations found by the model.

### Data sources
Prometheus uses several different types of input data, here's a quick run down.

#### Configuration File
This tsv-file defines what relations to train the model for.
The format is
```
<name> <wikidata relation id> <subject entity type> <object entity type>
```

Example: (Note that * means "any type")
```
place_of_birth  P19     PERSON  LOCATION
director        P57     PERSON  *
```

#### Corpus
A corpus annotated with part of speech, dependencies, named entities and named entities disambiguations stored in the Docforia documents in parquet format.

#### Wikidata
A dump of Wikidata stored in a Spark Dataframe in parquet.

#### Word2Vec
A Word2Vec model trained using the original C-implementation then translated into a more optimal format. See [Prometheus word2vec](https://github.com/ErikGartner/prometheus-word2vec) for details.

#### Model Evaluation Files
The evaluation files are modified versions of the evaluation results from the Google relation model found [here](https://research.googleblog.com/2013/04/50000-lessons-on-how-to-read-relation.html). They are created by feeding the original through the script in `scripts/convert_google_validation/`.

#### Vilde/Annotation Server
To annotate data on-the-fly we use the LTH server Vilde. If possible we'll release a Docker image of it.

### System Requirements
The Prometheus system is built for large-scale data processing and thus require a cluster. The word2vec model requires about 8 GB of RAM for English and even more is needed for memory caching of data.

The recommended amount of dedicated memory per worker is 32GB.

The minimum amount is about 20GB though that require some tweaking. Specifically you need to configure so that the system has 18GB of heap memory during all stages *except* during the ClassificationModel training, that stage requires about 6GB of heap memory and about 14GB of off-heap memory. See [neuralnet.md](./neuralnet.md).

Our cluster didn't have GPUs, so we do all training on CPUs. However it is possible to train on GPUs by changing the [DeepLearning4j](https://deeplearning4j.org/) dependencies from the CPU version to GPU version in the `pom.xml`-file.

## Installation
This program depends on several libraries using SBT as its dependency manager
and build tool. Only one dependency (Docforia) isn't publicly available through the Maven central repository.
So you need to build and install it from source.

1. Install [docforia](https://github.com/marcusklang/docforia) by cloning and then `mvn install`
2. Run `sbt compile`

## Running
After the dependencies are installed and the JVM memory is configured running the program is easy. Just supply the options as program arguments.

If you need to run the program on Spark the command `sbt pack` builds the jar and collects the dependencies to `target/pack/`.

From the `--help` summary, here are the parameters:
```
Usage: Prometheus [options] corpus-path config-file wikidata-path temp-data-path word2vecPath
Prometheus model trainer trains a relation extractor
Options:

  -c, --corefs                             enable co-reference resolutions for
                                           annotation
      --data-evaluation                    flag to evaluate extractions against
                                           Wikidata
  -d, --demo-server                        start an HTTP server to receive text
                                           to extract relations from
  -e, --epochs  <arg>                      number of epochs for neural network
  -l, --language  <arg>                    the language to use for the pipeline
                                           (defaults to en)
  -m, --model-evaluation-files  <arg>...   path to model evaluation files
  -n, --name  <arg>                        Custom Spark application name
  -p, --probability-cutoff  <arg>          use this to set the cutoff
                                           probability for extractions
  -s, --sample-size  <arg>                 use this to sample a fraction of the
                                           corpus
      --stage  <arg>                       how far to run the program,
                                           [preprocess|train|full]
train implies
                                           preprocess
full implies train

      --help                               Show help message
      --version                            Show version of this program

 trailing arguments:
  corpus-path (required)       path to the corpus to train on
  relation-config (required)   path to a TSV listing the desired relations to
                               train for
  wiki-data (required)         path to the wikidata dump in parquet
  temp-data-path (required)    path to a directory that will contain
                               intermediate results
  word2vec-path (required)     path to a word2vec model in the C binary format
```
Note that paths are written as either `hdfs:/`,  `s3:/` or `file:/` depending on where they are stored.

To run the program locally using SBT use:
```
sbt "run <arguments>"
```

There also exists help scripts (in `scripts/runc/`) for running on clusters such as AWS. These only help with uploading the jars, setting memory options and calling spark-submit. They are developed for internal usage and not documented here, however what they do is not complicated.

### Docker
A docker image exists that is configured to download the english models based of Wikipedia. The default command starts the REST api.

```bash
docker build -t prometheus/relation_extractor .
docker run -p 8080:8080 prometheus/relation_extractor
```

It is possible to perform training and run custom commands but it requires overriding the default commands using the `docker exec` command or by passing parameters to `docker run` which will override the sbt subcommand.

Depending on your system you might need to tweak the JVM options to allow for enough memory both off-heap for Javacpp and for the JVM. Define using the `--env <key>=<val>` flag to `docker run`.
By default the image uses 14GB of ram.

To not  download the word2vec model (about 5-10 GB) set the build-arg flag: `--build-arg DOWNLOAD_WORD2VEC=n`.

Working at Sony requires the usage of the corporate proxy. Use the custom make file to configure the Docker image to make
use of the proxy using the following command:

```bash
make -f scripts/sony/Makefile
```

## API Documentation
[ScalaDoc](https://erikgartner.github.io/prometheus-relation-model)

## Developers
This system was developed as a master's thesis for Agnot project of Sony Mobile Lund.
- [Axel Larsson](https://github.com/AxelTLarsson)
- [Erik Gärtner](https://github.com/ErikGartner)
