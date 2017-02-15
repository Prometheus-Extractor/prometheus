#!/bin/bash
VERSION=0.0.1-SNAPSHOT
JAR_NAME="prometheus-relation-model-assembly-"$VERSION".jar"
args=$@
WORK_PATH="~/projects"
REMOTE_HOST="exjobb8"
JVMOPTS="-XX:+AggressiveOpts -XX:+PrintFlagsFinal -XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35"

L_RED="\e[91m"
RES="\e[0m"
GREEN="\e[32m"
CYAN="\e[95m"

function test {
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
test sbt -Dmode=cluster assembly

printf "$GREEN == Uploading fat jar == $RES\n"
test scp target/scala-2.10/$JAR_NAME $REMOTE_HOST:$WORK_PATH/$JAR_NAME

printf "$GREEN == Running $JAR_NAME on exjobb8 == $RES\n"
test ssh $REMOTE_HOST 'bash' << EOF
cd $WORK_PATH
java $JVM_OPTS -jar $JAR_NAME $args
EOF

printf "$GREEN == Done == $RES\n"

