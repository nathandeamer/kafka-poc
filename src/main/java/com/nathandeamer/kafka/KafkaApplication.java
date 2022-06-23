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

		// Start sending messages to kafka:
		Executors.newSingleThreadExecutor().execute(() -> {
			Random random = new Random();
			int maxSeconds = 5;
			int minSeconds = 1;

			for (int i = 0; i < 10; i++) {
				try {
					//i++;
					producer.sendMessage(String.valueOf(i));
					int randomSeconds = random.nextInt((maxSeconds - minSeconds) + 1) + minSeconds;
					Thread.sleep(randomSeconds * 1000);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

}
