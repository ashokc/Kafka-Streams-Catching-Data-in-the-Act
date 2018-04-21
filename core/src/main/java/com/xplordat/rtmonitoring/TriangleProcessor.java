package com.xplordat.rtmonitoring ;

import org.apache.logging.log4j.Logger ;
import org.apache.logging.log4j.LogManager ;
import java.util.HashMap ;
import java.util.Iterator ;
import java.util.ArrayList ;
import java.util.List ;

import org.json.JSONObject ;
import org.json.JSONArray ;

import org.apache.kafka.streams.processor.ProcessorSupplier ;
import org.apache.kafka.streams.processor.Processor ;
import org.apache.kafka.streams.processor.ProcessorContext ;
import org.apache.kafka.streams.processor.PunctuationType ;

import org.apache.kafka.streams.state.KeyValueStore ;
import org.apache.kafka.streams.state.KeyValueIterator ;
import org.apache.kafka.streams.KeyValue ;

import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

import com.xplordat.rtmonitoring.avro.* ;
import org.apache.avro.io.JsonEncoder ;

public class TriangleProcessor implements ProcessorSupplier<String, SmoothedVertex>, Processor<String, SmoothedVertex> {
  private ProcessorContext context;
	private static final Logger logger = LogManager.getLogger(TriangleProcessor.class);
  Elastic elastic = new Elastic() ;

// windows

  private WindowStore<String, SmoothedVertex> smoothedVerticesWindowStateStore ;
	private ArrayList<JSONObject> triangleDocs = new ArrayList<JSONObject>() ;
	private ArrayList<JSONObject> smoothedVertexDocs = new ArrayList<JSONObject>() ;
	private String triangleIndex, smoothedIndex ;

	private long windowSize, windowRetention ;
	private long currentStreamTime = 0L ;
	HashMap<Long, HashMap<String, Long>> windowCounts = new HashMap<Long, HashMap<String, Long>>() ; // windowStartTime, sensor, count
	String[] allSensors = {"A", "B", "C"} ;
	private boolean resetStores, isThisANewProcessor ;

	TriangleProcessor (int windowSizeSeconds, int windowRetentionMinutes, boolean resetStores, String smoothedIndex, String triangleIndex) {
		windowSize = windowSizeSeconds * 1000L ;
		windowRetention = windowRetentionMinutes * 60L * 1000L ;
		this.resetStores = resetStores ;
		this.triangleIndex = triangleIndex ;
		this.smoothedIndex = smoothedIndex ;
		this.resetStores = resetStores ;
		isThisANewProcessor = true ;
	}

  @Override
	public Processor<String, SmoothedVertex> get() {
		return this ;
	}

  void restoreFromStore (long productionStartTime) {
		long timeStart = ( (productionStartTime / windowSize) - (24 * 3600 * 1000 / windowSize ) ) * windowSize ;
		while (timeStart <= productionStartTime) {
			long timeEnd = timeStart + windowSize ;
			long totalCount = 0L ;
			HashMap<String, Long> hm = new HashMap<String, Long>() ;
			for (String sensor: allSensors) {
				long count = 0L ; 
				hm.put(sensor, count) ;
    		WindowStoreIterator<SmoothedVertex> iter = smoothedVerticesWindowStateStore.fetch(sensor, timeStart, timeEnd) ;
    		while (iter.hasNext()) {
					count++ ;
					iter.next() ;
    		}
				iter.close() ;
				if (count > 0) {
					hm.put(sensor, count) ;
					totalCount = totalCount + count ;
					hm.put("total", totalCount) ;
				}
			}
			if (totalCount > 0) {
				windowCounts.put(timeStart, hm) ;
			}
			timeStart = timeEnd ;
		}
	}

	void computeTriangleMetrics (long windowKey) {
		long windowTimeStart = windowKey ;
		long windowTimeEnd = windowTimeStart + windowSize ;
		int aCount = 0 ; int bCount = 0 ; int cCount = 0 ;
		double ax = 0.0 ; double ay = 0.0 ; double bx = 0.0 ; double by = 0.0 ; double cx = 0.0 ; double cy = 0.0 ; long time = 0L ;
		double aDisplacement = 0.0 ; double bDisplacement = 0.0 ; double cDisplacement = 0.0 ;
		for (String sensor: allSensors) {
			Iterator<SmoothedVertex> itr = getItemsInWindow(sensor, windowTimeStart, windowTimeEnd).iterator() ;
			while (itr.hasNext()) {
				SmoothedVertex sv = itr.next() ;
				time = time + sv.getTimeInstant() ;
				if (sensor.equals("A")) {
					ax = ax + sv.getX() ;
					ay = ay + sv.getY() ;
					aDisplacement = aDisplacement + sv.getCumulativeDisplacementForThisSensor() ;
					aCount++ ;
				}
				else if (sensor.equals("B")) {
					bx = bx + sv.getX() ;
					by = by + sv.getY() ;
					bDisplacement = bDisplacement + sv.getCumulativeDisplacementForThisSensor() ;
					bCount++ ;
				}
				else if (sensor.equals("C")) {
					cx = cx + sv.getX() ;
					cy = cy + sv.getY() ;
					cDisplacement = cDisplacement + sv.getCumulativeDisplacementForThisSensor() ;
					cCount++ ;
				}
			}
		}
		if ( (windowCounts.get(windowKey).get("A") != aCount) || (windowCounts.get(windowKey).get("B") != bCount) || (windowCounts.get(windowKey).get("C") != cCount) ) {
			logger.error ("Count not matching in window key:" + windowKey) ; // This will only happen when 'timeInstant' falls exactly on the starting edge, a rare occurrentce
			logger.error ( windowCounts.get(windowKey).get("A") + "/" +  aCount + " " + windowCounts.get(windowKey).get("B") + "/" +  bCount + " " + windowCounts.get(windowKey).get("C") + "/" + cCount ) ;
		}
		if ( (aCount >= 1) && (bCount >= 1) && (cCount >= 1) ) {
			ax = ax / aCount ;	ay = ay / aCount ;	bx = bx / bCount ;	by = by / bCount ;	cx = cx / cCount ;	cy = cy / cCount ;				
			aDisplacement = aDisplacement / aCount ; bDisplacement = bDisplacement / bCount ; cDisplacement = cDisplacement / cCount ; 
			long timeInstant = time / (aCount + bCount + cCount) ;
			double totalDisplacement = aDisplacement + bDisplacement + cDisplacement ;
			double AB = findLength (ax, ay, bx, by) ;
			double BC = findLength (bx, by, cx, cy) ;
			double AC = findLength (ax, ay, cx, cy) ;
			double perimeter = AB + BC + AC ;
			double halfPerimeter = perimeter * 0.5 ;
			double area = Math.sqrt (halfPerimeter*(halfPerimeter-AB)*(halfPerimeter-BC)*(halfPerimeter-AC)) ;
			String triangleKey = windowTimeStart + "_" + windowTimeEnd ;
			Triangle triangle = new Triangle(windowTimeStart, timeInstant, windowTimeEnd, aCount, bCount, cCount, ax, ay, bx, by, cx, cy, AB, BC, AC, perimeter, area, totalDisplacement) ;
			long wallclock_span = windowCounts.get(windowKey).get("latestArrivalWallClockTime") - windowCounts.get(windowKey).get("earliestArrivalWallClockTime") ;
			JSONObject jo = new JSONObject(triangle.toString()).put("id", "triangle_" + triangleKey).put("wallclock_span", wallclock_span) ;
			triangleDocs.add(jo) ;
			if (triangleDocs.size() % 10 == 0) {
				indexThis (triangleDocs, triangleIndex) ;
				triangleDocs.clear() ;
			}
		}
		else {
			logger.info ("Too few entries in window...:" + windowKey + " => " + aCount + "/" + bCount + "/" +  cCount) ; // should not happen
		}
	}

	Double findLength (double x1, double y1, double x2, double y2) {	
		return Math.sqrt ( (y2-y1)*(y2-y1) + (x2-x1)*(x2-x1) ) ;
	}

  @Override
  @SuppressWarnings("unchecked")
  public void init(ProcessorContext context) {
		this.context = context;
		smoothedVerticesWindowStateStore = (WindowStore<String, SmoothedVertex>) context.getStateStore("smoothedVerticesWindowStateStore");
  }

	void indexThis (List<JSONObject> docs, String indexName) {
		try {
			elastic.indexNow (docs, indexName, "doc", "id") ;
		}
		catch (Exception e) {
     	logger.error ("Failed in bulk indexing..." + e) ;
		}
	}

  ArrayList<SmoothedVertex> getItemsInWindow (String sensor, long timeStart, long timeEnd) {
    ArrayList<SmoothedVertex> windowedData = new ArrayList<SmoothedVertex>() ;
    WindowStoreIterator<SmoothedVertex> iter = smoothedVerticesWindowStateStore.fetch(sensor, timeStart, timeEnd) ;
    while (iter.hasNext()) {
      windowedData.add(iter.next().value) ;
    }
    iter.close();
    return windowedData ;
	}

  void initializeWindow(long windowKey, long arrivalTime) {
    HashMap<String, Long> hm = new HashMap<String, Long>() ;
    for (String sensor: allSensors) {
      hm.put(sensor, 0L) ;
    }
    hm.put("total", 0L) ;
    hm.put("earliestArrivalWallClockTime", arrivalTime) ;
    windowCounts.put(windowKey, hm) ;
  }

	@Override
  public void process (String key, SmoothedVertex value) {
		long arrivalTime = System.currentTimeMillis() ;
		long delay = arrivalTime - value.getPushTime() ;
		long measurementTime = value.getTimeInstant() ;
		String incomingSensor = value.getSensor() ;
		if (isThisANewProcessor) {
			logger.info ("First measurement after re/start..." + value.toString()) ;
			if (!(resetStores)) {
				restoreFromStore(value.getTimeStart()) ;
			}
			else {
				double cy = 0.5 * Math.sqrt(3.0) ;
				double area0 = 0.5 * cy ;
				long productionStartTime = value.getTimeStart() ;
				Triangle triangle = new Triangle(productionStartTime, productionStartTime, productionStartTime, 0, 0, 0, 0.0, 0.0, 0.0, 1.0, 0.5, cy, 1.0, 1.0, 1.0, 3.0, area0, 0.0) ;
				JSONObject jo = new JSONObject(triangle.toString()).put("id", "triangle_" + productionStartTime + "_" + productionStartTime).put("wallclock_span", -1) ;
				triangleDocs.add(jo) ;
			}
			isThisANewProcessor = false ;	// the above block executes just once upon start up
		}
		smoothedVerticesWindowStateStore.put(incomingSensor, value, measurementTime) ;

		JSONObject jo = new JSONObject(value.toString()).put("id", incomingSensor + "_" + value.getPushCount()).put("delay", delay) ;
		smoothedVertexDocs.add(jo) ;
		if (smoothedVertexDocs.size() % 100 == 0) {
			indexThis (smoothedVertexDocs, smoothedIndex) ;
			smoothedVertexDocs.clear() ;
		}

		currentStreamTime = Math.max(currentStreamTime, measurementTime) ;
		long windowKey = (measurementTime / windowSize) * windowSize ;
		if (!(windowCounts.containsKey(windowKey))) {
			initializeWindow (windowKey, arrivalTime) ;
		}

		HashMap<String, Long> hm = windowCounts.get(windowKey) ;
		hm.put(incomingSensor, hm.get(incomingSensor) + 1L) ;
		hm.put("total", hm.get("total") + 1) ;
    hm.put("latestArrivalWallClockTime", arrivalTime) ;
		windowCounts.put(windowKey, hm) ;

		if ( (hm.get("A") > 0) && (hm.get("B") > 0) && (hm.get("C") > 0) ) { 
			computeTriangleMetrics (windowKey) ;
		}
  }
	
  @Override
  public void punctuate(long timestamp) {
      // this method is deprecated and should not be used anymore
  }

  @Override
  public void close() {
      // close the key-value store
			logger.info ("Closing smoothedVerticesWindowStateStore") ;
			if (smoothedVertexDocs.size() > 0) {
				indexThis (smoothedVertexDocs, smoothedIndex) ;
			}
			if (triangleDocs.size() > 0) {
				indexThis (triangleDocs, triangleIndex) ;
			}
      smoothedVerticesWindowStateStore.close();
  }

}
