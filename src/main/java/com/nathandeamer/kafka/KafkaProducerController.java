package com.nathandeamer.kafka;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

public class KafkaProducerController {

    @PostMapping
    public void publishMessage(@RequestBody String message) {
        // TODO:
    }

}
