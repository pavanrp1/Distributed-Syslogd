package org.vertx.kafka;

import org.junit.experimental.ParallelComputer;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;

public class KafkaMessageConsumerTestMain {

	public static void main(String[] args) {
		// JUnitCore.main("org.vertx.kafka.KafkaMessageConsumerTest","org.vertx.kafka.KafkaMessageConsumerTest2");

		Class[] cls = { KafkaMessageConsumerTest.class, KafkaMessageConsumerTest2.class,KafkaMessageConsumerTest3.class };

		// simultaneously all methods in all classes
		Result result = JUnitCore.runClasses(new ParallelComputer(true, true), cls);
		System.out.print(result.wasSuccessful());

		// simultaneously among classes
		// Result result = JUnitCore.runClasses(ParallelComputer.classes(), cls);

		// simultaneously among methods in a class
		// Result result = JUnitCore.runClasses(ParallelComputer.methods(), cls);

	}

}
