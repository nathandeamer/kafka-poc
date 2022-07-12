package com.nathandeamer.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class KafkaProducerController {

    private final Logger log = LoggerFactory.getLogger(KafkaProducerController.class);

    private final KafkaProducerService kafkaProducerService;

    public KafkaProducerController(KafkaProducerService kafkaProducerService) {
        this.kafkaProducerService = kafkaProducerService;
    }

    @PostMapping
    public void publishMessages() {
            for (int i = 0; i < 10; i++) {
                try {
                    // I've set the key to be the same, so all messages are written to the same partition for a demo.
                    kafkaProducerService.sendMessage("KEY", String.valueOf(i));
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }
        };
}
