#!/bin/bash

function monitor() { 
	clientId=$1
	acks=$2
	retries=$3
	enableIdempotence=$4
	maxInFlightRequestsPerConnection=$5
	sync=$6
	topics=$7
	sleepTimeMillis=$8
	amplitude=$9
# 2.0 * Math.PI * angularV / 60.0 => It will take 60secs i.e. 1 minute to trace the full circle. => period = 1 min
# If angularV = 2 then it wil take 30secs...
	angularV=${10}
# value = value * (1.0 + rand), where rand = error + random.nextDouble() * 2.0 * error ;
	error=${11}
	xReference=${12}
	yReference=${13}

	echo "java -cp ./core/target/rtmonitoring.jar com.xplordat.rtmonitoring.Producers $clientId $acks $retries $enableIdempotence $maxInFlightRequestsPerConnection $sync $topics $sleepTimeMillis $amplitude $angularV $error $xReference $yReference"

	java -cp ./core/target/rtmonitoring.jar com.xplordat.rtmonitoring.Producers $clientId $acks $retries $enableIdempotence $maxInFlightRequestsPerConnection $sync $topics $sleepTimeMillis $amplitude $angularV $error $xReference $yReference
}

monitor A all 0 false 1 true rawVertex 1000 1.0 1.0 0.001 0.0 0.0 &
monitor B all 0 false 1 true rawVertex 1000 1.0 1.0 0.001 1.0 0.0 &
monitor C all 0 false 1 true rawVertex 1000 1.0 1.0 0.001 0.5 0.866 &


