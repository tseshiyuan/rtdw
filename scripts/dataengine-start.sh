#!/bin/sh
export RUN_HOME=/Users/albin/Desktop/Workshop/Works/SagLi/lubeinsightsplatform/run
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_05.jdk/Contents/Home/
$JAVA_HOME/bin/java -cp $RUN_HOME/../out/production/lubeinsightsplatform:$RUN_HOME/../lib/*:$RUN_HOME/../lib/spark/*:$RUN_HOME \
-Dlog4j.configuration=file:///$RUN_HOME/log4j.properties -Dservice.conf=$RUN_HOME/service.conf com.saggezza.lubeinsights.platform.core.script.EngineStart dataengine &
