package com.okeydokey.study.apachekafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@SpringBootApplication
public class ApacheKafkaApplication {

    public static void main(String[] args) {
        SpringApplication.run(ApacheKafkaApplication.class, args);
    }

    public static final String topicName = "slipp";

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @PostMapping("/send")
    public void sendMessage(@RequestBody String msg) {
        kafkaTemplate.send(topicName, msg);
    }

    @KafkaListener(topics = topicName, groupId = "foo")
    public void listen(String message) {
        System.out.println("Received Message in group foo1: " + message);
    }

    @KafkaListener(topics = topicName, groupId = "foo")
    public void listen1(String message) {
        System.out.println("Received Message in group foo2: " + message);
    }

    @KafkaListener(topics = topicName, groupId = "bar")
    public void listen2(String message) {
        System.out.println("Received Message in group bar1: " + message);
    }

    @KafkaListener(topics = topicName, groupId = "bar")
    public void listen3(String message) {
        System.out.println("Received Message in group bar2: " + message);
    }

    @KafkaListener(topics = topicName, groupId = "baz")
    public void listenWithHeaders(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
        System.out.println(
                "Received Message in group baz1: " + message + " from partition: " + partition);
    }
}
