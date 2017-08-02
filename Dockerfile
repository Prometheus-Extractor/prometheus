FROM openjdk:8
LABEL maintainer "erikgartner@sony.com"

ENV SCALA_VERSION 2.10.6
ENV SBT_VERSION 0.13.8
ENV DOCFORIA_TAG e3d2005b8c359cf4eb1e0d90932df79e2fe4ad2e
ARG DOWNLOAD_WORD2VEC=y
ARG MAVEN_OPTS=''
ARG SBT_OPTS=''

# Scala expects this file
RUN touch /usr/lib/jvm/java-8-openjdk-amd64/release

# Install Scala
## Piping curl directly in tar
RUN \
  curl -fsL http://downloads.typesafe.com/scala/$SCALA_VERSION/scala-$SCALA_VERSION.tgz | tar xfz - -C /root/ && \
  echo >> /root/.bashrc && \
  echo 'export PATH=~/scala-$SCALA_VERSION/bin:$PATH' >> /root/.bashrc

# Install sbt
RUN \
  curl -L -o sbt-$SBT_VERSION.deb http://dl.bintray.com/sbt/debian/sbt-$SBT_VERSION.deb && \
  dpkg -i sbt-$SBT_VERSION.deb && \
  rm sbt-$SBT_VERSION.deb && \
  apt-get update && \
  apt-get install sbt && \
  sbt sbtVersion

# Install docforia
RUN \
  apt-get install -y maven
RUN git clone https://github.com/marcusklang/docforia.git /root/docforia
WORKDIR /root/docforia
RUN git checkout $DOCFORIA_TA
RUN mvn install

# Copy scripts
COPY scripts ./scripts

# Prepare data
RUN mkdir /data
WORKDIR /data
RUN mkdir -p prometheus/en
RUN mkdir -p word2vec/en
RUN mkdir corpus
RUN mkdir wikidata
RUN touch config.tsv

# Download pretrained models from AWS
COPY scripts/docker/download_model.sh ./
RUN ./download_model.sh

# Copy app
RUN mkdir /app
WORKDIR /app
COPY src ./src
COPY project ./project
COPY .sbtopts .env build.sbt ./

# Install sbt plugins and compile software
RUN sbt compile

# Make data accessible as a volume to allow custom data.
VOLUME /data

ENV SPARK_MASTER "local[*]"
EXPOSE 8080

WORKDIR /app
ENTRYPOINT ["sbt"]
CMD ["run -c -d -l en file:/data/corpus file:/data/config.tsv file:/data/wikidata file:/data/prometheus file:/data/word2vec/en"]
