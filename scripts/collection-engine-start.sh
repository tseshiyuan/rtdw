#!/bin/sh

if [ -z "$PROG_HOME" ] ; then
  ## resolve links - $0 may be a link to PROG_HOME
  PRG="$0"

  # need this for relative symlinks
  while [ -h "$PRG" ] ; do
    ls=`ls -ld "$PRG"`
    link=`expr "$ls" : '.*-> \(.*\)$'`
    if expr "$link" : '/.*' > /dev/null; then
      PRG="$link"
    else
      PRG="`dirname "$PRG"`/$link"
    fi
  done

  saveddir=`pwd`

  PROG_HOME=`dirname "$PRG"`/..

  # make it fully qualified
  PROG_HOME=`cd "$PROG_HOME" && pwd`

  cd "$saveddir"
fi


${PROG_HOME}/target/pack/bin/engine-start collectionengine