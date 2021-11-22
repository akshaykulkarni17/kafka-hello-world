package com.example.kafka.streams;


import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamsFilterTweets {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,"demo-kafka-streams");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.StringSerde.class.getName());

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String,String> inputTopic = streamsBuilder.stream("twitter-tweets");
        KStream<String,String> filteredStream = inputTopic.filter(
                (k,jsonTweet) -> extractFollowers(jsonTweet) > 10000
        );
        filteredStream.to("important_topics");

        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(),properties);

        kafkaStreams.start();


    }

    private static int extractFollowers(String jsonTweet) {
        return JsonParser.parseString(jsonTweet)
                .getAsJsonObject().get("user")
                .getAsJsonObject().get("followers_count")
                .getAsInt();
    }
}
