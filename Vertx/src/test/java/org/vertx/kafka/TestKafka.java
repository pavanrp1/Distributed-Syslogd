package org.vertx.kafka;

public class TestKafka {

	public static void main(String[] args) {
		for (int i = 0; i < 1; i++) {
			KafkaMessageConsumer kafka = new KafkaMessageConsumer();
			kafka.main(args);
		}
	}

}
