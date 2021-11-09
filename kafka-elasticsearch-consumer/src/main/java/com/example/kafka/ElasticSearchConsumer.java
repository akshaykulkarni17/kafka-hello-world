package com.example.kafka;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ElasticSearchConsumer {

    final  static Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class);
    public static RestHighLevelClient createClient(){


        String hostName = "kafka-hello-world-7247708859.us-east-1.bonsaisearch.net";
        String userName = "10dcezl4kp";
        String password = "m419rl5r6b";

        final CredentialsProvider credentialsProvider =  new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,new UsernamePasswordCredentials(userName,password));
        RestClientBuilder builder = RestClient.builder(new HttpHost(hostName,443,"https")).setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
            @Override
            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            }
        });
        return new RestHighLevelClient(builder);
    }

    public static KafkaConsumer<String,String> createConsumer(String topic){
        String bootstrapServer = "localhost:9092";
        String groupId = "kafka-elasticsearch";

        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        //properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
        //properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"10");

        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Collections.singleton(topic));
        return consumer;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        RestHighLevelClient client = createClient();

        //String json = "{\"foo\":\"bar\"}";




        KafkaConsumer<String,String> consumer = createConsumer("twitter-tweets");
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            logger.info("Total records "+records.count());
            for (ConsumerRecord<String,String> record : records) {
                String json = record.value();
                IndexRequest indexRequest = new IndexRequest("twitter","tweets",extractId(json))
                        .source(json, XContentType.JSON );

                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                logger.info("id: "+indexResponse.getId());
                Thread. sleep(1001);
            }

        }
        //client.close();

    }

    private static String extractId(String json) {
        return JsonParser.parseString(json).getAsJsonObject().get("id_str").getAsString();
    }
}
