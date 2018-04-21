package com.xplordat.rtmonitoring ;

import org.apache.kafka.streams.processor.TimestampExtractor ;
import org.apache.kafka.clients.consumer.ConsumerRecord ;

import org.apache.logging.log4j.Logger ;
import org.apache.logging.log4j.LogManager ;

import com.xplordat.rtmonitoring.avro.* ;
import org.apache.avro.io.JsonEncoder ;

public class StreamTimeStampExtractor implements TimestampExtractor {
  private static final Logger logger = LogManager.getLogger(StreamTimeStampExtractor.class);

  @Override
  public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp) {
		long timeStamp = System.currentTimeMillis() ;
		Object obj = record.value() ;
		if (obj != null) {
			if (obj instanceof RawVertex) {
				RawVertex rv = (RawVertex) obj ;
				timeStamp = rv.getTimeInstant() ;
			}
			else if (obj instanceof SmoothedVertex) {
				SmoothedVertex sv = (SmoothedVertex) obj ;
				timeStamp = sv.getTimeInstant() ;
			}
			else {
				logger.error ("THIS SHOULD NOT HAPPEN... returning current time rather than timeInstant") ;
			}
		}
		return timeStamp ;
  }
}


