package com.nathandeamer.kafka;

import org.apache.kafka.common.errors.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Service
public class KafkaProducerService {

    private final Logger log = LoggerFactory.getLogger(KafkaProducerService.class);

    @Value(value = "${kafka.topic-name}")
    private String topicName;


    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaProducerService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(String key, String message) throws Exception {
        try {
            SendResult<String, String> sendResult = kafkaTemplate.send(topicName, key, message).get(1000L, TimeUnit.MILLISECONDS);
            sendResult.getRecordMetadata().partition();

            System.out.println("Sent message=[" + message + "] to partition=[" + sendResult.getRecordMetadata().partition()+ "] with offset=[" + sendResult.getRecordMetadata().offset() + "]");

        } catch (ExecutionException | InterruptedException e) {
            log.error("Execution/Interrupted Exception and the details is {}", e.getMessage());
            throw e;
        } catch (TimeoutException e) {
            log.error("Timeout Exception and the details is {}", e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            log.error("Execution sending the message and the details are {}", e.getMessage(), e);
            throw e;
        }


        /*
        ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(topicName, message);

        future.addCallback(new ListenableFutureCallback<>() {
            @Override
            public void onSuccess(SendResult<String, String> result) {
                System.out.println("Sent message=[" + message + "] with offset=[" + result.getRecordMetadata().offset() + "] to partition=[" + result.getRecordMetadata().partition() + "]");
            }

            @Override
            public void onFailure(Throwable ex) {
                System.out.println("Unable to send message=[" + message + "] due to : " + ex.getMessage());
            }
        });
        */
    }

}
