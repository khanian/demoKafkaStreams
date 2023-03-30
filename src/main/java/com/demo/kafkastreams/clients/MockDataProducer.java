package com.demo.kafkastreams.clients;

import com.demo.kafkastreams.util.DataGenerator;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class MockDataProducer {

    private static final Logger LOG = LoggerFactory.getLogger(MockDataProducer.class);
    private static Producer<String, String> producer;
    private static Callback callback;
    private static final String YELLING_APP_TOPIC = "src-topic";
    private static final int YELLING_APP_ITERATIONS = 5;
    private static ExecutorService executorService = Executors.newFixedThreadPool(1);
    private static volatile boolean keepRunning = true;

    public static void produceRandomTextData() {
        Runnable generateTask = () -> {
            init();
            int counter = 0;
            while (counter++ < YELLING_APP_ITERATIONS) {
                List<String> textValues = DataGenerator.generateRandomText();

                for (String value : textValues) {
                    ProducerRecord<String, String> record = new ProducerRecord<>(YELLING_APP_TOPIC, null, value);
                    producer.send(record, callback);
                }
                LOG.info("Text batch sent");
                try {
                    Thread.sleep(6000);
                } catch (InterruptedException e) {
                    Thread.interrupted();
                }
            }
            LOG.info("Done generating text data");

        };
        executorService.submit(generateTask);
    }

    private static void init() {
        if (producer == null) {
            LOG.info("Initializing the producer");
            Properties properties = new Properties();
            properties.put("bootstrap.servers", "localhost:9092");
            properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            properties.put("acks", "1");
            properties.put("retries", "3");

            producer = new KafkaProducer<>(properties);

            callback = (metadata, exception) -> {
                if (exception != null) {
                    exception.printStackTrace();
                }
            };
            LOG.info("Producer initialized");
        }
    }

    public static void shutdown() {
        LOG.info("Shutting down data generation");
        keepRunning = false;

        if (executorService != null) {
            executorService.shutdownNow();
            executorService = null;
        }
        if (producer != null) {
            producer.close();
            producer = null;
        }
    }

}
