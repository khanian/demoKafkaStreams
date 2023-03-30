package com.demo.kafkastreams.chapter3;

import com.demo.kafkastreams.clients.MockDataProducer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaStreamsYellingApp {
    private static final Logger log = LoggerFactory.getLogger(KafkaStreamsYellingApp.class);

    public static void main(String[] args) throws InterruptedException {

        MockDataProducer.produceRandomTextData();
        
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "yelling_app_id");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        StreamsConfig streamsConfig = new StreamsConfig(props);

        Serde<String> stringSerde = Serdes.String();

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> simpleFirstStream = builder.stream("src-topic", Consumed.with(stringSerde, stringSerde));

        KStream<String, String> upperCasedStream = simpleFirstStream.mapValues((ValueMapper<String, String>) String::toUpperCase);

        upperCasedStream.to( "out-topic", Produced.with(stringSerde, stringSerde));
        upperCasedStream.print(Printed.<String, String>toSysOut().withLabel("Yelling App"));


        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(),streamsConfig);
        log.info("Hello World Yelling App Started");
        kafkaStreams.start();
        Thread.sleep(35000);
        log.info("Shutting down the Yelling APP now");
        kafkaStreams.close();
        MockDataProducer.shutdown();

    }
}
