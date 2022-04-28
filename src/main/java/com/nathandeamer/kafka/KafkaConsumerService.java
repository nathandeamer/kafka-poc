package com.nathandeamer.kafka;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerService {

    @KafkaListener(topics = "${kafka.topic-name}")
    public void listenWithHeaders(
            @Payload String message,
            @Header(KafkaHeaders.OFFSET) int offset,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {

        System.out.println("Received message=[" + message + "] with offset=[" + offset + "] from partition=[" + partition+ "]");
    }

}
