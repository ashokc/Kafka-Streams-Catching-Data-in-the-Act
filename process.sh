#!/bin/bash

startStream=$1

streamProcess() {
	applicationId=$1
	clientId=$2
	nthreads=$3
	guarantee=$4										# at_least_once OR exactly_once
	commit_interval_ms=$5						#	300,000				OR 100	(smaller value makes sense as exactly_once needs to KNOW)
	resetStores=$6
	windowSize=$7						# seconds
	windowRetention=$8			# minutes
	smoothIngInterval=$9		#	seconds
	smoothedIndex=${10}
  triangleIndex=${11}

	echo "java -cp ./core/target/rtmonitoring.jar com.xplordat.rtmonitoring.StreamProcess $applicationId $clientId $nthreads $guarantee $commit_interval_ms $resetStores $windowSize, $windowRetention, $smoothIngInterval, $smoothedIndex, $triangleIndex"
	java -cp ./core/target/rtmonitoring.jar com.xplordat.rtmonitoring.StreamProcess $applicationId $clientId $nthreads $guarantee $commit_interval_ms $resetStores $windowSize $windowRetention $smoothIngInterval $smoothedIndex $triangleIndex
}

if [ "$startStream" == "start" ]; then
	resetStores="true"
elif [ "$startStream" == "resume" ]; then
	resetStores="false"
else
	echo "Need 1 arg start/resume"
	exit
fi

streamProcess triangle-stress-monitor 0 3 at_least_once 100 $resetStores 60 360 12 smoothed triangle


