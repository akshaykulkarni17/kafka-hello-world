package com.example.kafka.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    final Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

    private String consumerKey = "hNpjOLMGTUttEMsX5XR0lTFII";
    private String consumerSecret = "VrXorS1Oz9vBW60enVO7k9vNi2zhRI2ZMyKLfjFbhzfEpNvbHd";
    private String token = "125370746-VXJ392N1NbA9FWPmWq2BOtIBQgnRhslVxJwTi4nm";
    private String secret = "6DJHz7AG6OWSJYAFQXOJrHLKfIvwDCwfmgDZsS6S209Tg";
    
    public TwitterProducer() {
    }

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    

    private void run() {
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);

        // Attempts to establish a connection.
        Client client = createTwitterClient(msgQueue);
        client.connect();

        //Create kafka producer
        KafkaProducer<String,String> tweetProducer = createTwitterProducer();

        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            logger.info("Stopping Application");
            client.stop();
            tweetProducer.close();
        }));

        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if (msg!=null){
                logger.info("Message: " +msg);
                tweetProducer.send(new ProducerRecord<>("twitter-tweets",null, msg), (recordMetadata, e) -> {
                    if (e==null){
                        logger.info("Message sent to Topic: "+recordMetadata.topic() +" Partition: "+ recordMetadata.partition());
                    }
                    else {
                        logger.error("Something bad happened"+e);
                    }
                });
            }

        }

    }

    private KafkaProducer<String, String> createTwitterProducer() {

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        return new KafkaProducer<String, String>(properties);

    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue){

        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        List<String> terms = Lists.newArrayList("india","polkadot");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file

        Authentication hosebirdAuth = new OAuth1(consumerKey,consumerSecret, token,secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();


    }

    

}
