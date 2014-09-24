#!/bin/sh
export RUN_HOME=/Users/chiyao/dev/lubeinsightsplatform/run
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_20.jdk/Contents/Home
$JAVA_HOME/bin/java -cp $RUN_HOME/lubeinsightsplatform.jar -Dservice.conf=$RUN_HOME/service.conf com.saggezza.lubeinsights.platform.core.serviceutil.ServiceCatalog $1 &
