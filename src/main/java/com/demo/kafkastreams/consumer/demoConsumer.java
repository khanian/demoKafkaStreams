package com.demo.kafkastreams.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class demoConsumer {

    @KafkaListener(id="src-listener", topics = "src-topic")
    public void listenSrc(String message) {
        System.out.println("Listener :: src-topic message = " + message);
    }

    @KafkaListener(id="out-listener", topics = "out-topic")
    public void listenOut(String message) {
        System.out.println("Listener :: out-topic message = " + message);
    }
}
