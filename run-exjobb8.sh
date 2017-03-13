#!/bin/bash
VERSION=0.1.0
JAR_NAME="prometheus-relation-model-assembly-"$VERSION".jar"
args=$@

WORK_PATH="~/projects"
REMOTE_HOST="exjobb8"
SPARK_SUBMIT="~/projects/spark-1.6.3-bin-hadoop2.6/bin/spark-submit"

#Controls the max resultsize for a collect()
SPARK_MAX_RESULTSIZE="8192m"

#This is not the fastest GC, but works well under heavy GC load.
JVMOPTS="-XX:+AggressiveOpts -XX:+PrintFlagsFinal -XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35"

# Pretty colours
L_RED="\e[91m"
RES="\e[0m"
GREEN="\e[32m"
CYAN="\e[95m"

function execute {
  "$@"
  local status=$?
  if [ $status -ne 0 ]; then
    printf "$L_RED Error with $1$RES\n" >&2
    printf "$CYAN Status code: $status$RES\n"
    exit $status
  fi

  return $status
}

printf "$GREEN == Bulding fat jar == $RES\n"
execute sbt -Dmode=cluster assembly

printf "$GREEN == Uploading fat jar == $RES\n"
execute scp target/scala-2.10/$JAR_NAME $REMOTE_HOST:$WORK_PATH/$JAR_NAME

printf "$GREEN == Running $JAR_NAME on exjobb8 == $RES\n"
execute ssh $REMOTE_HOST 'bash -s' << EOF
  cd $WORK_PATH
  $SPARK_SUBMIT --conf spark.driver.maxResultSize=$SPARK_MAX_RESULTSIZE \
    --conf spark.executor.extraJavaOptions="$JVMOPTS" $JAR_NAME $args
EOF
printf "$GREEN == Done == $RES\n"

