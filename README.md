# kafka-hello-world

### Module 1 : Kafka Basics (Producer consumer tests)

### Module 2 : Twitter Kafka Producer (Fetches tweets with keywords)

### Module 3 : Elastic search consumer (Fetches tweets produced by Twitter Kafka Producer and puts them into elastic search)

### Module 4 : Kafka Streams API to filter tweets and publish to another topic

### Module 5 : Kafka connect-standalone for twitter 

###### Running on Local Environment
1. Start the zookeeper
   * _zookeeper-server-start config/zookeeper.properties_
2. Start the kafka server
   * _kafka-server-start config/server.properties_
3. Run the application