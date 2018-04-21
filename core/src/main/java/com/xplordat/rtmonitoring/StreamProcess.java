package com.xplordat.rtmonitoring ;

import org.apache.logging.log4j.Logger ;
import org.apache.logging.log4j.LogManager ;

import java.io.InputStream ;
import java.util.Properties ;
import java.util.Date ;
import java.util.Iterator ;
import java.util.Collections ;

import java.util.concurrent.* ;

import java.util.Map ;
import java.util.Set ;
import java.util.List ;
import java.util.ArrayList ;
import java.util.HashMap ;
import java.util.concurrent.TimeUnit ;

import com.xplordat.rtmonitoring.avro.* ;

// For the stores
import org.apache.kafka.common.serialization.Serdes;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.KeyValueStore ;
import org.apache.kafka.streams.state.WindowStore ;

// For the sources & sinks
import org.apache.kafka.common.serialization.StringSerializer ;
import org.apache.kafka.common.serialization.StringDeserializer ;
import org.apache.kafka.common.serialization.Serde;

import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

// Streams
import org.apache.kafka.streams.Topology ;
import org.apache.kafka.streams.KafkaStreams;

public class StreamProcess {

  Properties streamProps = new Properties() ;
  private static final Logger logger = LogManager.getLogger(StreamProcess.class);
	boolean resetStores ;
	Map<String, String> kadsConfig = new HashMap<String, String>() ;

	int windowSizeSeconds, windowRetentionMinutes, smoothingIntervalSeconds ;
	String smoothedIndex, triangeIndex ;

  public static void main (String[] args) {
    if (args.length == 11) {
      StreamProcess streamProcess = new StreamProcess() ;
      streamProcess.streamProps.setProperty("application.id", args[0]) ;
      streamProcess.streamProps.setProperty("client.id", args[1]) ;
			streamProcess.streamProps.setProperty("num.stream.threads",args[2]) ;
			streamProcess.streamProps.setProperty("processing.guarantee",args[3]) ;
			streamProcess.streamProps.setProperty("commit.interval.ms",args[4]) ;
			streamProcess.resetStores = Boolean.parseBoolean(args[5]) ;
			streamProcess.windowSizeSeconds = Integer.parseInt(args[6]) ;
			streamProcess.windowRetentionMinutes = Integer.parseInt(args[7]) ;
			streamProcess.smoothingIntervalSeconds= Integer.parseInt(args[8]) ;
			streamProcess.smoothedIndex = args[9] ;
			streamProcess.triangeIndex = args[10] ;

			streamProcess.streamProps.setProperty("bootstrap.servers","localhost:9092") ;
			streamProcess.streamProps.setProperty("schema.registry.url", "http://localhost:8081");
			streamProcess.streamProps.setProperty("default.timestamp.extractor", StreamTimeStampExtractor.class.getName());

      streamProcess.go() ;
    }
    else {
      System.out.println ("USAGE: java -cp ./core/target/kafka.jar com.xplordat.kafka.StreamProcess $applicationId $clientId $threads $guaranteea $commitInterval $resetStrores") ;
    }
  }

	KafkaAvroDeserializer getKafkaAvroDeserializer() {
		KafkaAvroDeserializer kads = new KafkaAvroDeserializer() ;
		kads.configure(kadsConfig, false) ;
		return kads ;
	}
	KafkaAvroSerializer getKafkaAvroSerializer() {
		KafkaAvroSerializer kas = new KafkaAvroSerializer() ;
		kas.configure(kadsConfig, false) ;
		return kas ;
	}
	void setKadsConfig() {
		kadsConfig.put("schema.registry.url", "http://localhost:8081");
		kadsConfig.put("specific.avro.reader", "true") ;
	}

  void go() {
		Map<String, String> schemaConfig = Collections.singletonMap("schema.registry.url", "http://localhost:8081");

		Serde<RawVertex> rawVertexAvroSerde = new SpecificAvroSerde<>() ;
		rawVertexAvroSerde.configure(schemaConfig, false) ;		// This "false" is for the variable "final boolean isSerdeForRecordKeys"

		Serde<SmoothedVertex> smoothedVertexAvroSerde = new SpecificAvroSerde<>() ;
		smoothedVertexAvroSerde.configure(schemaConfig, false) ;		// This "false" is for the variable "final boolean isSerdeForRecordKeys"

		try {
			StoreBuilder<KeyValueStore<Long, RawVertex>> vertexProcessorRawVertexKVStateStoreBuilder = Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("rawVertexKVStateStore"), Serdes.Long(), rawVertexAvroSerde) ;

			StoreBuilder<KeyValueStore<Long, SmoothedVertex>> vertexProcessorSmoothedVertexKVStateStoreBuilder = Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("smoothedVertexKVStateStore"), Serdes.Long(), smoothedVertexAvroSerde) ;

			StoreBuilder<WindowStore<String, SmoothedVertex>> triangleProcessorWindowStateStoreBuilder = Stores.windowStoreBuilder(Stores.persistentWindowStore("smoothedVerticesWindowStateStore", TimeUnit.MINUTES.toMillis(windowRetentionMinutes), 3, TimeUnit.SECONDS.toMillis(windowSizeSeconds),false), Serdes.String(), smoothedVertexAvroSerde) ;

			Topology topology = new Topology();

			setKadsConfig() ;
			topology.addSource("vertexSource", new StringDeserializer(), getKafkaAvroDeserializer(), "rawVertex")
				.addProcessor("vertexProcessor", () -> new VertexProcessor(smoothingIntervalSeconds, resetStores), "vertexSource")
				.addStateStore(vertexProcessorRawVertexKVStateStoreBuilder, "vertexProcessor")
				.addStateStore(vertexProcessorSmoothedVertexKVStateStoreBuilder, "vertexProcessor")
				.addSink("smoothedVerticesSink", "smoothedVertex", new StringSerializer(), getKafkaAvroSerializer(), "vertexProcessor")
				.addSource("smoothedVerticesSource", new StringDeserializer(), getKafkaAvroDeserializer(), "smoothedVertex")
				.addProcessor("triangleProcessor", () -> new TriangleProcessor(windowSizeSeconds, windowRetentionMinutes, resetStores, smoothedIndex, triangeIndex), "smoothedVerticesSource")
				.addStateStore(triangleProcessorWindowStateStoreBuilder, "triangleProcessor") ;

			logger.info(topology.describe());

      KafkaStreams kstreams = new KafkaStreams(topology, streamProps);
			if (resetStores) {
				kstreams.cleanUp();
			}
			kstreams.start() ;

      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          try {
						logger.error ("Interrupting stream processing...") ;
            kstreams.close() ;
          }
          catch (Exception e) {
            logger.error ("Errors in shutdownhook..." + e) ;
            System.exit(1) ;
          }
        }
      }) ;
		}
		catch (Exception e) {
			logger.error ("Some error in streamkafka", e) ;
		}
	}

}

