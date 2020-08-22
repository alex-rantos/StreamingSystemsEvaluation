#!/bin/bash

if [[ $# -ne 1 ]]; then
    echo $0' needs exactly 1 argument to run.'
    echo 'Please specify the script name to replace for flink build path.'
    exit 0
fi

build() {
    cp $1 /$1"_backup"
    sed -i "s/FLINK_BUILD_PATH=.*/FLINK_BUILD_PATH="'"'"\/flink\/flink-1.4.1\/"'"'"/g" $1
    sed -i "s/JAR_PATH=.*/JAR_PATH="'"'"\/flink\/ds2-master\/flink-examples\/target\/flink-examples-1.0-SNAPSHOT-jar-with-dependencies.jar"'"'"/g" $1   
}

build $1
echo "sed.sh - All set"