package com.nathandeamer.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.Random;
import java.util.concurrent.Executors;

@SpringBootApplication
public class KafkaApplication {

	public static void main(String[] args) {

		ConfigurableApplicationContext context = SpringApplication.run(KafkaApplication.class, args);
		KafkaProducerService producer = context.getBean(KafkaProducerService.class);

		// TODO: FOrce onto the same partition by setting the order as being important.

		// Start sending messages to kafka:
		Executors.newSingleThreadExecutor().execute(() -> {
			Random random = new Random();
			int maxSeconds = 5;
			int minSeconds = 1;

			for (int i = 0; i < 10; i++) {
				try {
					producer.sendMessage("KEY", String.valueOf(i)); // I've set the key to be the same, so all messages are written to the same partition for a demo.
					int randomSeconds = random.nextInt((maxSeconds - minSeconds) + 1) + minSeconds;
					Thread.sleep(randomSeconds * 1000);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});

	}

}
