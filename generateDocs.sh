#!/bin/bash
SCC_HOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
OUTPUT="/tmp/SCC_DOC_TEMP"

if [ "$#" -lt 1 ]; then
    echo "At least one version parameter is required, e.g. 3.0.1 (no leading 'v')."
    exit 1
fi


rm -r $OUTPUT
mkdir -p $OUTPUT
echo "SPARK CASSANDRA CONNECTOR HOME IS $SCC_HOME"
for VERSION in $@ ;do
    echo "Making docs for $VERSION"
    git checkout "v$VERSION"
    if [ $? -ne 0 ]; then
        echo "Unable to checkout version $VERSION, skipping"
        continue
    fi
    sbt clean
    sbt doc
    mkdir $OUTPUT/$VERSION

    for MODULE in connector driver test-support; do
        FOLDER=$SCC_HOME/$MODULE
        echo "COPYING $FOLDER to $OUTPUT/$VERSION/$MODULE"
        cp -vr $FOLDER/target/scala-2.13/api $OUTPUT/$VERSION/$MODULE
    done
done
git checkout gh-pages
cp -r $OUTPUT/* $SCC_HOME/ApiDocs
