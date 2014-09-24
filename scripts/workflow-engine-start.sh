#!/bin/sh
export RUN_HOME=/Users/chiyao/dev/lubeinsightsplatform/run
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_20.jdk/Contents/Home
export LIB_HOME=/Users/chiyao/dev/lubeinsightsplatform/out/artifacts/lubeinsightsplatform_jar
$JAVA_HOME/bin/java -cp $RUN_HOME/lubeinsightsplatform.jar:$LIB_HOME/* -Dlog4j.configuration=file:///$RUN_HOME/log4j.properties -Dservice.conf=$RUN_HOME/service.conf \
 com.saggezza.lubeinsights.platform.core.script.EngineStart workflowengine &
