# Donwload Prometheus pretrained models and encoder.
AWS_BUCKET="https://s3-eu-west-1.amazonaws.com/sony-prometheus-data/docker_data"

cd prometheus/en/
curl -fsL $AWS_BUCKET"/prometheus/en/dep_encoder.tar.gz" | \
  tar xfz - -C ./

curl -fsL $AWS_BUCKET"/prometheus/en/netype_encoder.tar.gz" | \
  tar xfz - -C ./

curl -fsL $AWS_BUCKET"/prometheus/en/pos_encoder.tar.gz" | \
  tar xfz - -C ./

curl -fsL $AWS_BUCKET"/prometheus/en/filter_model.tar.gz" | \
  tar xfz - -C ./

curl -fsL $AWS_BUCKET"/prometheus/en/classification_model.tar.gz" | \
  tar xfz - -C ./

cd ../..
cd word2vec/en
#curl -fsLO $AWS_BUCKET"/word2vec/en/model.opt.vecs"
#curl -fsLO $AWS_BUCKET"/word2vec/en/model.opt.vocab"

cd ../../
curl -fsLO $AWS_BUCKET"/config.tsv"
