#!/bin/sh
export RUN_HOME=/Users/chiyao/dev/lubeinsightsplatform/run
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_20.jdk/Contents/Home
export COLLECTOR_NAME=myCollector
$JAVA_HOME/bin/java -cp $RUN_HOME/lubeinsightsplatform.jar -Dcollector.name=$COLLECTOR_NAME -Dlog4j.configuration=file:///$RUN_HOME/log4j.properties -Dkafka.conf=$RUN_HOME/kafka.conf com.saggezza.lubeinsights.platform.core.collectionengine.FileCollector $1  &
