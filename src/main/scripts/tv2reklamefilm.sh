#!/bin/sh

BASEDIR=$(readlink -f $(dirname $0))/..

java -classpath $BASEDIR/lib/*:$BASEDIR/config dk.statsbiblioteket.doms.ingest.reklamepbcoremapper.Tv2PBCoreMapperUtil "$@"