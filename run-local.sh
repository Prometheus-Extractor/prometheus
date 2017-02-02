#!/bin/bash
TERM=xterm-color
args=$@

#The jar to execute
JAR_NAME="fact-extractor-1.0-SNAPSHOT.jar"

#Userdefined args
JAR_USER_ARGS="--local"

JVMOPTS="-Xmx4g"

#Controls the max resultsize for a collect()
SPARK_MAX_RESULTSIZE="1024m"

function test {
	"$@"
	local status=$?
	if [ $status -ne 0 ]; then
		echo "error with $1" >&2
		echo "Status code: $status"
		exit $status
	fi

	return $status
}

#This is not the fastest GC, but works well under heavy GC load.
JVMOPTS+=" -XX:+AggressiveOpts -XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:InitiatingHeapOccupancyPercent=35"

echo " == Running command == "
java $JVMOPTS -jar target/$JAR_NAME $JAR_USER_ARGS $args
echo " == Done. == "
