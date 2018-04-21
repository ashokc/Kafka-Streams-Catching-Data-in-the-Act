package com.xplordat.rtmonitoring ;

import org.apache.logging.log4j.Logger ;
import org.apache.logging.log4j.LogManager ;
import java.util.Properties ;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import com.xplordat.rtmonitoring.avro.* ;

public class Producers {

	Properties producerProps = new Properties() ; 
	private static final Logger logger = LogManager.getLogger(Producers.class);
	String topics ;
	double amplitude, angularV, error, xReference, yReference ;
	boolean sync ;
	long sleepTimeMillis ;
	public static void main (String[] args) {
		if (args.length == 13) {
			Producers producers = new Producers() ;
			producers.producerProps.setProperty("client.id", args[0]) ;
			producers.producerProps.setProperty("acks", args[1]) ;
			producers.producerProps.setProperty("retries", args[2]) ;
			producers.producerProps.setProperty("enable.idempotence", args[3]) ;
			producers.producerProps.setProperty("max.in.flight.requests.per.connection", args[4]) ;
			producers.sync = Boolean.parseBoolean(args[5]) ;
			producers.topics = args[6] ;
			producers.sleepTimeMillis = Long.parseLong(args[7]) ;
			producers.amplitude = Double.parseDouble(args[8]) ;
			producers.angularV = Double.parseDouble(args[9]) ;
			producers.error = Double.parseDouble(args[10]) ;
			producers.xReference = Double.parseDouble(args[11]) ;
			producers.yReference = Double.parseDouble(args[12]) ;

			producers.producerProps.setProperty("bootstrap.servers","localhost:9092") ;
			producers.producerProps.setProperty("key.serializer","io.confluent.kafka.serializers.KafkaAvroSerializer") ;
			producers.producerProps.setProperty("value.serializer","io.confluent.kafka.serializers.KafkaAvroSerializer") ;
			producers.producerProps.setProperty("schema.registry.url","http://localhost:8081") ;

			producers.go() ;
		}
		else {
			System.out.println ("USAGE: java -cp ./core/target/kafka.jar com.xplordat.kafka.rtmonitoring.producers $clientId $acks $retries $enableIdempotence $maxInFlightRequestsPerConnection $sync $topics $sleepTimeMillis $amplitude $angularV $error $xReference $yReference") ;
		}
	}

	void go() {
		String[] topicsList = topics.split(",") ;

		ProducerProcess producerProcess = new ProducerProcess (topicsList, sync, producerProps, sleepTimeMillis, amplitude, angularV, error, xReference, yReference) ;
		Thread producerThread = new Thread(producerProcess, "thread-"+producerProps.getProperty("client.id")) ;	
		producerThread.start() ;
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				try {
					producerThread.interrupt() ;
					producerThread.join() ;
				}
				catch (Exception e) {
					logger.error ("Errors in shutdownhook..." + e) ;
					System.exit(1) ;
				}
			}
		});
	}

}

