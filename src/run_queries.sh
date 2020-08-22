#!/bin/bash

if [[ $# -le 0 ]] && [[ $# -ge 3 ]]; then
    echo 'This script needs exactly 1 or 2 argument to run.'
    echo 'Please specify the query name.'
    exit 0
fi

# Contant paths
FLINK_DS2=/flink/ds2-master/flink-scaling-scripts/
FLINK_NEX=/flink/ds2-master/flink-examples/src/main/java/ch/ethz/systems/strymon/ds2/flink/nexmark/scripts/

### paths configuration ###
FLINK_BUILD_PATH="/flink/flink-1.4.1/"
FLINK=$FLINK_BUILD_PATH$"bin/flink"
JAR_PATH="/flink/ds2-master/flink-examples/target/flink-examples-1.0-SNAPSHOT-jar-with-dependencies.jar"
BUILD_HELPER="/bin/bash /now/sed.sh"

### Helpers scripts ###
python_monitor_script="python3 /now/monitor.py"
script_path="${0}"

executeQuery3() {
    QUERY_CLASS="ch.ethz.systems.strymon.ds2.flink.nexmark.queries.$1"
    A_RATE='20000'
    P_RATE='10000'
    PA_SOURCE='1'
    PP_SOURCE='1'
    P_JOIN='1'
    ${BUILD_HELPER} $script_path
    if [ $# -eq 3 ]; then
        IFS='#' read -r -a array <<< "$2"
        for element in "${array[@]}"
        do
            IFS=',' read -r -a parallelism <<< "$element"
            if [ "${parallelism[0]}" == "auctionRate" ]; then
                echo "Auction source rate: ${parallelism[@]: -1:1}"
                A_RATE="${parallelism[@]: -1:1}"
            fi
            if [ "${parallelism[0]}" == "personRate" ]; then
                echo "Person source rate: ${parallelism[@]: -1:1}"
                P_RATE="${parallelism[@]: -1:1}"
            fi
            if [ "${parallelism[0]}" == "pAuctionSource" ]; then
                echo "Custon Source Auctions: ${parallelism[@]: -1:1}"
                PA_SOURCE="${parallelism[@]: -1:1}"
            fi
            if [ "${parallelism[0]}" == "pPersonSource" ]; then
                echo "Custon Source Persons: ${parallelism[@]: -1:1}"
                PP_SOURCE="${parallelism[@]: -1:1}"
            fi
            if [ "${parallelism[0]}" == "pjoin" ]; then
                echo "Incremental join & Sink ${parallelism[@]: -1:1}"
                P_JOIN="${parallelism[@]: -1:1}"
            fi
        done
        echo "Auction source rate:${A_RATE}, Person source rate :${P_RATE}, Custon Source Auctions: ${PA_SOURCE}, Custon Source Persons: ${PP_SOURCE}, Incremental join & Sink: ${P_JOIN}"
        nohup $FLINK run -d --class $QUERY_CLASS $JAR_PATH --auction-srcRate $A_RATE --person-srcRate $P_RATE --p-auction-source $PA_SOURCE --p-person-source $PP_SOURCE --p-window $P_JOIN & > job.out

    else
        echo "$1 will run with default parameters:"
        echo "Source rate:${S_RATE}, Bidder Source :${P_SOURCE}, FlatMapBidder & DummyLatencySink: ${P_MAP}"
        nohup $FLINK run -d --class $QUERY_CLASS $JAR_PATH --auction-srcRate $A_RATE --person-srcRate $P_RATE --p-auction-source $PA_SOURCE --p-person-source $PP_SOURCE --p-window $P_JOIN & > job.out
    fi
    $python_monitor_script $3 $1 $A_RATE $P_RATE $PA_SOURCE $PP_SOURCE $P_JOIN
}


echo "Argument: $1 || $2 has been inserted. Running ${0}"
case "$1" in
    Query1)
        QUERY_CLASS="ch.ethz.systems.strymon.ds2.flink.nexmark.queries.$1"
        EX_RATE='0.82'
        S_RATE='100000'
        P_SOURCE='1'
        P_MAP='1'
        ${BUILD_HELPER} $script_path
        if [ $# -eq 3 ]; then
            IFS='#' read -r -a array <<< "$2"
            for element in "${array[@]}"
            do
                IFS=',' read -r -a parallelism <<< "$element"
                ## search for exchange-rate
                if [ "${parallelism[0]}" == "exRate" ]; then
                    echo "Exchange rate: ${parallelism[@]: -1:1}"
                    EX_RATE="${parallelism[@]: -1:1}"
                fi
                ## search for source rate
                if [ "${parallelism[0]}" == "srcRate" ]; then
                    echo "Source rate: ${parallelism[@]: -1:1}"
                    S_RATE="${parallelism[@]: -1:1}"
                fi
                ## search for Bids source
                if [ "${parallelism[0]}" == "psource" ]; then
                    echo "Bids Source: ${parallelism[@]: -1:1}"
                    P_SOURCE="${parallelism[@]: -1:1}"
                fi
                ## search for Mapper and Latency Sink
                if [ "${parallelism[0]}" == "pmap" ]; then
                    echo "Mapper & Latency Sink: ${parallelism[@]: -1:1}"
                    P_MAP="${parallelism[@]: -1:1}"
                fi
            done
            nohup $FLINK run -d --class $QUERY_CLASS $JAR_PATH --exchange-rate $EX_RATE --srcRate $S_RATE --p-source $P_SOURCE --p-map $P_MAP & > job.out

        else
            echo "Query will run with default parameters:"
            echo "Exchage rate:${EX_RATE}, Source rate:${S_RATE}, Bidder Source :${P_SOURCE}, Mapper and Latency Sink: ${P_MAP}"
            nohup $FLINK run -d --class $QUERY_CLASS $JAR_PATH --exchange-rate $EX_RATE --srcRate $S_RATE --p-source $P_SOURCE --p-map $P_MAP & > job.out
        fi
        $python_monitor_script $3 $1 $EX_RATE $S_RATE $P_SOURCE $P_MAP
        ;;

    Query2)
        QUERY_CLASS="ch.ethz.systems.strymon.ds2.flink.nexmark.queries.$1"
        S_RATE='100000'
        P_SOURCE='1'
        P_MAP='1'
        ${BUILD_HELPER} $script_path
        if [ $# -eq 3 ]; then
            IFS='#' read -r -a array <<< "$2"
            for element in "${array[@]}"
            do
                IFS=',' read -r -a parallelism <<< "$element"
                ## search for source rate
                if [ "${parallelism[0]}" == "srcRate" ]; then
                    echo "Source rate: ${parallelism[@]: -1:1}"
                    S_RATE="${parallelism[@]: -1:1}"
                fi
                ## search for Bids source
                if [ "${parallelism[0]}" == "psource" ]; then
                    echo "Bids Source: ${parallelism[@]: -1:1}"
                    P_SOURCE="${parallelism[@]: -1:1}"
                fi
                ## search for FlatMap and DummyLatencySink
                if [ "${parallelism[0]}" == "pmap" ]; then
                    echo "FlatMapBidder & DummyLatencySink: ${parallelism[@]: -1:1}"
                    P_MAP="${parallelism[@]: -1:1}"
                fi
            done
            nohup $FLINK run -d --class $QUERY_CLASS $JAR_PATH --srcRate $S_RATE --p-source $P_SOURCE --pmap $P_MAP & > job.out

        else
            echo "Query will run with default parameters:"
            echo "Source rate:${S_RATE}, Bidder Source :${P_SOURCE}, FlatMapBidder & DummyLatencySink: ${P_MAP}"
            nohup $FLINK run -d --class $QUERY_CLASS $JAR_PATH --srcRate $S_RATE --p-source $P_SOURCE --pfmap $P_MAP & > job.out

        fi
        $python_monitor_script $3 $1 $S_RATE $P_SOURCE $P_MAP
        ;;
    Query3)
        executeQuery3 $1 $2 $3
        ;;
    Query3Stateful)
        executeQuery3 $1 $2 $3
        ;;
    Query5)
        QUERY_CLASS="ch.ethz.systems.strymon.ds2.flink.nexmark.queries.$1"
        S_RATE='100000'
        P_SOURCE='1'
        P_WINDOW='1'
        ${BUILD_HELPER} $script_path
        if [ $# -eq 3 ]; then
            IFS='#' read -r -a array <<< "$2"
            for element in "${array[@]}"
            do
                IFS=',' read -r -a parallelism <<< "$element"
                if [ "${parallelism[0]}" == "srcRate" ]; then
                    echo "Source rate: ${parallelism[@]: -1:1}"
                    S_RATE="${parallelism[@]: -1:1}"
                fi
                if [ "${parallelism[0]}" == "p-bid-source" ]; then
                    echo "Bids Source: ${parallelism[@]: -1:1}"
                    P_SOURCE="${parallelism[@]: -1:1}"
                fi
                if [ "${parallelism[0]}" == "p-window" ]; then
                    echo "Sliding Window/DummyLatencySink: ${parallelism[@]: -1:1}"
                    P_WINDOW="${parallelism[@]: -1:1}"
                fi
            done
            nohup $FLINK run -d --class $QUERY_CLASS $JAR_PATH --srcRate $S_RATE --p-bid-source $P_SOURCE --p-window $P_WINDOW & > job.out

        else
            echo "Query5 will run with default parameters:"
            #echo "Source rate:${S_RATE}, Bidder Source :${P_SOURCE}, FlatMapBidder & DummyLatencySink: ${P_MAP}"
            nohup $FLINK run -d --class $QUERY_CLASS $JAR_PATH --srcRate $S_RATE --p-bid-source $P_SOURCE --p-window $P_WINDOW & > job.out

        fi
        $python_monitor_script $3 $1 $S_RATE $P_SOURCE $P_WINDOW
        ;;
    Query8)
        executeQuery3 $1 $2
        ;;
    Query11)
        QUERY_CLASS="ch.ethz.systems.strymon.ds2.flink.nexmark.queries.$1"
        S_RATE='100000'
        P_SOURCE='1'
        P_WINDOW='1'
        ${BUILD_HELPER} $script_path
        if [ $# -eq 3 ]; then
            IFS='#' read -r -a array <<< "$2"
            for element in "${array[@]}"
            do
                IFS=',' read -r -a parallelism <<< "$element"
                if [ "${parallelism[0]}" == "srcRate" ]; then
                    echo "Source rate: ${parallelism[@]: -1:1}"
                    S_RATE="${parallelism[@]: -1:1}"
                fi
                if [ "${parallelism[0]}" == "p-bid-source" ]; then
                    echo "Bids Source: ${parallelism[@]: -1:1}"
                    P_SOURCE="${parallelism[@]: -1:1}"
                fi
                if [ "${parallelism[0]}" == "p-window" ]; then
                    echo "Session Window/DummyLatencySink: ${parallelism[@]: -1:1}"
                    P_WINDOW="${parallelism[@]: -1:1}"
                fi
            done
            nohup $FLINK run -d --class $QUERY_CLASS $JAR_PATH --srcRate $S_RATE --p-bid-source $P_SOURCE --p-window $P_WINDOW & > job.out

        else
            echo "$1 will run with default parameters:"
            #echo "Source rate:${S_RATE}, Bidder Source :${P_SOURCE}, FlatMapBidder & DummyLatencySink: ${P_MAP}"
            nohup $FLINK run -d --class $QUERY_CLASS $JAR_PATH --srcRate $S_RATE --p-bid-source $P_SOURCE --p-window $P_WINDOW & > job.out

        fi
        $python_monitor_script $3 $1 $S_RATE $P_SOURCE $P_WINDOW
        ;;
    word)
        cp /flink/ds2-master/flink-scaling-scripts/start-wordcount.sh /flink/ds2-master/flink-scaling-scripts/start-wordcount.sh_backup
        sed -i "s/FLINK_BUILD_PATH="/flink/flink-1.4.1/"
        sed -i "s/JAR_PATH="/flink/ds2-master/flink-examples/target/flink-examples-1.0-SNAPSHOT-jar-with-dependencies.jar"
        /flink/ds2-master/flink-scaling-scripts/start-wordcount.sh "Source: Custom Source",1#"Splitter FlatMap",1#"Count -> Latency Sink",1
        exit 1 ;;
    *) echo 'Given argument is invalid. Exiting ${0}' ;;
esac
