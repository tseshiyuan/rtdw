#!/bin/sh
export RUN_HOME=/Users/chiyao/dev/lubeinsightsplatform/run
export OUTPUT=$RUN_HOME/engine.out
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_20.jdk/Contents/Home
$JAVA_HOME/bin/java -cp $RUN_HOME/lubeinsightsplatform.jar -Dlog4j.configuration=file:///$RUN_HOME/log4j.properties -Dservice.conf=$RUN_HOME/service.conf -Dkafka.conf=$RUN_HOME/kafka.conf com.saggezza.lubeinsights.platform.core.script.EngineStart $1 2>&1 >$OUTPUT &
