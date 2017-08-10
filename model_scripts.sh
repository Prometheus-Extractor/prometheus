function backup_model() {

  LANG=$1
  NAME=$2
  DESCRIPTION=$3
  DATE=$(date "+%Y-%m-%d-%H%M")

  BUCKET="s3://sony-prometheus-data"
  BUCKET_SRC="$BUCKET/relation_model/$LANG"
  BUCKET_DEST="$BUCKET/archive/$LANG/$DATE----$NAME"

  echo "Backing up model to: $BUCKET_DEST"
  aws s3 cp --recursive $BUCKET_SRC/filter_model $BUCKET_DEST/filter_model
  aws s3 cp --recursive $BUCKET_SRC/classification_model $BUCKET_DEST/classification_model
  aws s3 cp --recursive $BUCKET_SRC/dep_encoder $BUCKET_DEST/dep_encoder
  aws s3 cp --recursive $BUCKET_SRC/netype_encoder $BUCKET_DEST/netype_encoder
  aws s3 cp --recursive $BUCKET_SRC/pos_encoder $BUCKET_DEST/pos_encoder
  aws s3 cp $BUCKET/configs/target_relations_$LANG.tsv $BUCKET_DEST/target_relations_$LANG.tsv

}
