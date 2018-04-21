# Kafka-Streams-Catching-Data-in-the-Act

This is the source code to along with the series of blog articles

* [Kafka Streams - Catching Data in the Act. 1](http://xplordat.com/2018/03/10/kafka-streams-catching-data-in-the-act-1/)
* [Kafka Streams - Catching Data in the Act. 2: Steady and Unsteady States](http://xplordat.com/2018/04/11/kafka-streams-catching-data-in-the-act-2-steady-and-unsteady-states/)
* [Kafka Streams - Catching Data in the Act. 3: The Mechanics](http://xplordat.com/2018/04/11/kafka-streams-catching-data-in-the-act-3-the-mechanics/)

The code employs,

* Kafka (9092) & Schema Registry (8081) on localhost
* Elasticsearch (localhost:9200) for long term storage. If not needed, exclude 'Elastic.java' and all references to 'indexThis' in 'TriangleProcessor.java'. 

## Usage

1. Create the "rawVertex" & "smoothedVertex" topics.
		
		./topics.sh

2. Start the three producers A, B & C. They generate raw positional data as the vertices move around. Modify the arguments in the script to simulate a faster or slower clip.

		./produce.sh

3. Start the stream process. This smmoths the raw vertex data & computes triangle metrics. Again, modify the arguments in the script to change the smoothing intercal windowstore behavior, number of stream threads etc...

		./process.sh start

4. To resume an interrupted stream process...

		./process.sh resume

5. If Elasticsearch storage & Kibana have been enabled, you can follow the metrics at:

		http://localhost:5601


