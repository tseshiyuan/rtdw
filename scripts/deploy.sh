# a simple deployment script: copy everything ffrmo build directory to run time directory
# please modify BUILD_HOME and RUN_HOME as you need
BUILD_HOME=/Users/chiyao/dev/lubeinsightsplatform
RUN_HOME=/Users/chiyao/dev/lubeinsightsplatform/run
cp $BUILD_HOME/conf/* $RUN_HOME
cp $BUILD_HOME/scripts/* $RUN_HOME
cp $BUILD_HOME/target/lubeinsightsplatform-assembly-1.0-SNAPSHOT.jar $RUN_HOME/lubeinsightsplatform.jar


