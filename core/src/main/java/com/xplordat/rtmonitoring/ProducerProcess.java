package com.xplordat.rtmonitoring ;

import org.apache.logging.log4j.Logger ;
import org.apache.logging.log4j.LogManager ;
import java.util.Properties ;
import java.util.Random ;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata ;
import org.apache.kafka.clients.producer.Callback ;

import org.json.JSONObject ;
import org.json.JSONArray ;

import com.xplordat.rtmonitoring.avro.* ;

public class ProducerProcess implements Runnable {

//	Y = A * sin (w*t).	w: angular velocity. radinans/second 
//	angularV = 2.0 * Math.PI / 60.0 ;	It will take 60secs i.e. 1 minute to trace the full circle. => period = 1 min

	private static final Logger logger = LogManager.getLogger(ProducerProcess.class);
	String[] topics ;
	boolean sync ;
	Properties producerProps ;
	long sleepTimeMillis ;
	private double amplitude, angularV, error, xReference, yReference ;
	private Random random = new Random() ;
 	private Producer<String, RawVertex> producer ;
	private int partitionNumber ;
	Elastic elastic = new Elastic() ;

	ProducerProcess (String[] topics, boolean sync, Properties producerProps, long sleepTimeMillis, double amplitude, double angularV, double error, double xReference, double yReference) {
    this.topics = topics ;
    this.sync = sync;
    this.producerProps = producerProps ;
		this.sleepTimeMillis = sleepTimeMillis ;
		this.amplitude = amplitude ;
		this.angularV = angularV * 2.0 * Math.PI / 60.0 ;			// one revolution a minute
		this.error = error ;
		this.xReference = xReference ;
		this.yReference = yReference ;
 		producer = new KafkaProducer<String, RawVertex>(producerProps);
	}

	@Override
	public void run() {
		String clientId = producerProps.getProperty("client.id") ;
		if (clientId.equals("A")) {
			partitionNumber = 0 ;
		}
		else if (clientId.equals("B")) {
			partitionNumber = 1 ;
		}
		else if (clientId.equals("C")) {
			partitionNumber = 2 ;
		}
		long timeStart = System.currentTimeMillis() ; // current time in milliseconds
		long timePrev = timeStart ;
		double valX = xReference ;
		double valY = yReference ;
		try {
			logger.info ("-------------- Kafka Producer Start:" + producerProps.getProperty("client.id") + " ----------------") ;
			while ( !(Thread.currentThread().isInterrupted()) ) {
				for (String topic: topics) {
					String key = clientId ;
					long currentTime = System.currentTimeMillis() ;
					double rand = -error + random.nextDouble() * 2.0 * error ;
					valX = valX + amplitude * Math.sin(angularV * (currentTime - timePrev) * 0.001) * rand ;
					valY = valY + amplitude * Math.cos(angularV * (currentTime - timePrev) * 0.001) * rand ;
					RawVertex rawVertex = new RawVertex (clientId, currentTime, valX, valY) ;
					ProducerRecord<String, RawVertex> record = new ProducerRecord<>(topic, partitionNumber, key, rawVertex) ;
					if (sync) {
						RecordMetadata metadata = producer.send(record).get();
					}
    			else {
						producer.send(record, new KafkaCallback(producerProps.getProperty("acks"))) ;
					}
					timePrev = currentTime ;
				}
				Thread.sleep(sleepTimeMillis) ;
      }
		}
		catch (InterruptedException e) {
			logger.error ("Producer " + producerProps.getProperty("client.id") + " Was Interrupted", e) ;
		}
		catch (Exception e) {
			logger.error ("Producer " + producerProps.getProperty("client.id") + " ran into some errors", e) ;
		}
		finally {
    	producer.close();
		}
	}
}

class KafkaCallback implements Callback {
  private static final Logger logger = LogManager.getLogger(KafkaCallback.class);
	String acks ;
	KafkaCallback (String acks) {
		this.acks = acks ;
	}
  public void onCompletion (RecordMetadata metadata, Exception e) {
    if (e != null) {
      e.printStackTrace();
    }
  }
}

