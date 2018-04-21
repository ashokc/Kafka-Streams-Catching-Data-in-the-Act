package com.xplordat.rtmonitoring ;
import org.apache.logging.log4j.Logger ;
import org.apache.logging.log4j.LogManager ;
import java.util.List ;
import java.util.Iterator ;
import java.net.InetAddress ;
import org.elasticsearch.transport.client.PreBuiltTransportClient ;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings ;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.action.bulk.BulkRequestBuilder ;
import org.elasticsearch.action.bulk.BulkResponse ; 
import org.elasticsearch.action.bulk.BulkItemResponse ;
import org.elasticsearch.common.xcontent.XContentType ;
import org.json.JSONObject ;

public class Elastic {
	private static final Logger logger = LogManager.getLogger(Elastic.class);
  private TransportClient client ;
  public Elastic () {
		Settings settings = Settings.builder().put("cluster.name", "es-6.1.2").put("client.transport.sniff", true).build();
    try {
      client = (new PreBuiltTransportClient(settings)).addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));
    }
    catch (Exception e) {
      logger.fatal("Unable to set up a connection to Elasticsearch cluster...", e) ;
    }
  }
  public void closeClient() {
    client.close() ;
  }
  public TransportClient getESClient() { 
    return client ;
  }

  public BulkResponse indexBulk (String indexName, List<JSONObject> docs, String docType, String idField) throws Exception {
    BulkRequestBuilder bulkRequestBuilder = client.prepareBulk() ;
    Iterator<JSONObject> itr = docs.iterator() ;
    while (itr.hasNext()) {
      JSONObject doc = itr.next() ;
			bulkRequestBuilder.add(client.prepareIndex(indexName,docType,doc.getString(idField)).setSource(doc.toString(), XContentType.JSON)) ;
    }
    return bulkRequestBuilder.execute().actionGet();
  }

  public int indexNow (List<JSONObject> docs, String indexName, String type, String idField) throws Exception {
    int failedDocs = 0 ;
    try {
      BulkResponse bulkResponse = indexBulk (indexName, docs, type, idField) ;
      if (bulkResponse.hasFailures()) {
        Iterator<BulkItemResponse> itr = bulkResponse.iterator() ;
        while (itr.hasNext()) {
          BulkItemResponse bir = itr.next() ;
          int rowNumber = bir.getItemId() ;
          if (bir.isFailed()) {
            logger.error("Error: ActionType=" + bir.getType() + " FailureMessage=" +  bir.getFailureMessage() + "\tResponse=" + bir.getResponse() + "\tRecord=" + docs.get(rowNumber).toString()) ;
            failedDocs = failedDocs + 1 ;
          }
        }
      }
			return failedDocs ;
    }
    catch (Exception elasticIndexingError) {
      logger.error ("Issues in elastic indexing... Exiting...", elasticIndexingError) ;
			throw new Exception (elasticIndexingError) ;
    }
  }
}
