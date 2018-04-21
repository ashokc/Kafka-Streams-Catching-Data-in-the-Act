package com.xplordat.rtmonitoring ;

import org.apache.logging.log4j.Logger ;
import org.apache.logging.log4j.LogManager ;

import org.apache.kafka.streams.processor.ProcessorSupplier ;
import org.apache.kafka.streams.processor.Processor ;
import org.apache.kafka.streams.state.KeyValueStore ;
import org.apache.kafka.streams.processor.ProcessorContext ;
import org.apache.kafka.streams.processor.PunctuationType ;
import org.apache.kafka.streams.state.KeyValueIterator ;
import org.apache.kafka.streams.KeyValue ;
import com.xplordat.rtmonitoring.avro.* ;
import org.apache.kafka.streams.processor.TimestampExtractor ;
import org.apache.kafka.clients.consumer.ConsumerRecord ;

public class VertexProcessor implements ProcessorSupplier<String, RawVertex>, Processor<String, RawVertex> {
  private ProcessorContext context;
  private KeyValueStore<Long, RawVertex> rawVertexKVStateStore ;
  private KeyValueStore<Long, SmoothedVertex> smoothedVertexKVStateStore ;
	private static final Logger logger = LogManager.getLogger(VertexProcessor.class);

	private String sensor ;
	private Long timeInstant, pushCount ;
	private Long timeStart = Long.MAX_VALUE ;
	private Long timeEnd = Long.MIN_VALUE ;
	private Long numberOfSamplesIntheAverage = 0L ;
	private Double X0, Y0, xSum, ySum ;
	private long smoothingInterval ;
	private boolean resetStores, isThisANewProcessor ;
	private boolean startASmoothingBatch = false ;

  @Override
	public Processor<String, RawVertex> get() {
		return this ;
	}

	VertexProcessor (int smoothingIntervalSeconds, boolean resetStores) {
		smoothingInterval = smoothingIntervalSeconds * 1000L ;
		this.resetStores = resetStores ;
		isThisANewProcessor = true ;
	}

	void restoreFromStore() {
		numberOfSamplesIntheAverage = 0L ;
    KeyValueIterator<Long, RawVertex> iter0 = rawVertexKVStateStore.all();
		xSum = 0.0 ;
		ySum = 0.0 ;
    while (iter0.hasNext()) {
			RawVertex rv = iter0.next().value ;
			xSum = xSum + rv.getX() ;
			ySum = ySum + rv.getY() ;
			timeStart = Math.min(timeStart, rv.getTimeInstant()) ;
			timeEnd = Math.max(timeEnd, rv.getTimeInstant()) ;
			numberOfSamplesIntheAverage++ ;
		}
		iter0.close() ;
	}

  void emptyTheRawStore () {
    KeyValueIterator<Long, RawVertex> iter0 = rawVertexKVStateStore.all();
		int count = 0 ;
    while (iter0.hasNext()) {
			Long key = iter0.next().key ;
			rawVertexKVStateStore.delete(key) ;
			count++ ;
   	}
    iter0.close();
		if (numberOfSamplesIntheAverage != count) {
   		logger.info ("Emptied the raw store: present/removed do NOT match => " + numberOfSamplesIntheAverage + "/" + count) ;
		}
		numberOfSamplesIntheAverage = 0L ;
	}

	void initialize() {
		numberOfSamplesIntheAverage = 0L ;
		pushCount = 0L ;
		if (sensor.equals("A")) {
			X0 = 0.0 ;
			Y0 = 0.0 ;
		}
		else if (sensor.equals("B")) {
			X0 = 1.0 ;
			Y0 = 0.0 ;
		}
		else if (sensor.equals("C")) {
			X0 = 0.5 ;
			Y0 = 0.5 * Math.sqrt(3.0) ;
		}
		xSum = 0.0 ;
		ySum = 0.0 ;
	}

  double findLength (double x1, double y1, double x2, double y2) {
    return Math.sqrt ( (y2-y1)*(y2-y1) + (x2-x1)*(x2-x1) ) ;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void init(ProcessorContext context) {
		this.context = context;
		rawVertexKVStateStore = (KeyValueStore) context.getStateStore("rawVertexKVStateStore");
		smoothedVertexKVStateStore = (KeyValueStore) context.getStateStore("smoothedVertexKVStateStore");
		context.schedule(smoothingInterval, PunctuationType.STREAM_TIME, (timestamp) -> {
			if (numberOfSamplesIntheAverage > 0L) {
				SmoothedVertex prevLsv = smoothedVertexKVStateStore.get(1L) ;
				double prevX = prevLsv.getX() ;
				double prevY = prevLsv.getY() ;
				pushCount = prevLsv.getPushCount() + 1 ;
				double X = xSum / numberOfSamplesIntheAverage ;
				double Y = ySum / numberOfSamplesIntheAverage ;
				double cumulativeDisplacementForThisSensor = prevLsv.getCumulativeDisplacementForThisSensor() + findLength (X, Y, prevX, prevY) ;
				SmoothedVertex sv = new SmoothedVertex (sensor, timeStart, timeInstant, timeEnd, X, Y, numberOfSamplesIntheAverage, cumulativeDisplacementForThisSensor, pushCount, System.currentTimeMillis()) ;
				context.forward(sensor, sv) ; // Forward it to "smoothedVertex" topic. 
				startASmoothingBatch = true ;
				smoothedVertexKVStateStore.put(1L, sv) ;	// Update the smoothedVertexKVStateStore
				context.commit();
			}
		});
  }

	@Override
  public void process(String key, RawVertex measurement) {
   	sensor = measurement.getSensor().toString() ;
		timeInstant = measurement.getTimeInstant() ;
		boolean discardThisMeasurement = false ;
		if (isThisANewProcessor) {			// this is the start of this processor task
			if (resetStores) {			// need to start over with clean stores
				startASmoothingBatch = true ;
				initialize() ;
				timeStart = timeInstant - 1000 ;	// 1 sec before the real measurements... WE WANT each timeinstant forwarded for a sensor to be different... 
				timeEnd = timeStart ;
				SmoothedVertex sv0 = new SmoothedVertex (sensor, timeStart, timeStart, timeStart, X0, Y0, 0L, 0.0, 0L, System.currentTimeMillis()) ;
				context.forward(sensor, sv0) ;		// Forward the initial SmoothedVertex to "smoothed-measurements" topic/sink so globalProcessor can get it
     		smoothedVertexKVStateStore.put(1L, sv0) ;
				resetStores = false ;
			}
			else {		// there is an existing store... pick up from there
				startASmoothingBatch = false ;
				restoreFromStore() ;
			}
			isThisANewProcessor = false ;		// the above block will only run once... as the processor task comes on line
		}

		if (startASmoothingBatch) {	// new smoothing batch
			xSum = 0.0 ;
			ySum = 0.0 ;
			emptyTheRawStore() ;
			timeStart = timeInstant ;
			timeEnd = timeStart ;
			startASmoothingBatch = false ;
		}
		else {
			long timeStart1 = Math.min(timeInstant, timeStart) ;
			long timeEnd1 = Math.max(timeInstant, timeEnd) ;
			if ( (timeEnd1 - timeStart1) > smoothingInterval ) { // too late or early arriving measurements ... does not happen often
				discardThisMeasurement = true ;
				logger.info ("Discarding the incoming measurement... timeStart/timeEnd > smoothingInterval:" + timeStart1 + "/" + timeEnd1 ) ;
				logger.info ("measurement:" + measurement.toString()) ;
			}
			else {
				timeEnd = timeEnd1 ;
				timeStart = timeStart1 ;
			}
		}

		if ( !(discardThisMeasurement) ) {
			numberOfSamplesIntheAverage++ ;
			timeInstant = (timeEnd + timeStart) / 2 ;
			xSum = xSum + measurement.getX() ;
			ySum = ySum + measurement.getY() ;
			RawVertex rv = new RawVertex(sensor, timeInstant, measurement.getX(), measurement.getY()) ;
			rawVertexKVStateStore.put(numberOfSamplesIntheAverage, rv) ;
		}
	}

  @Override
  public void punctuate(long timestamp) {
      // this method is deprecated and should not be used anymore
  }

  @Override
  public void close() {
		logger.info ("Closing the stores...") ; // close all the stores
		rawVertexKVStateStore.close();
		smoothedVertexKVStateStore.close();
  }

}
