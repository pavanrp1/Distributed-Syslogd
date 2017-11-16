package org.vertx.kafka.consumer;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Properties;
import java.util.function.Consumer;

//import KafkaProducer packages
import org.apache.kafka.clients.producer.KafkaProducer;
//import simple producer packages
import org.apache.kafka.clients.producer.Producer;
//import ProducerRecord packages
import org.apache.kafka.clients.producer.ProducerRecord;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.EventBusOptions;

public class SimpleProducer {

	private static Vertx vertx;

	public static void main(String[] args) throws Exception {

		String topicName = "testGroup";

		topicName = "syslogd";

		Properties props = new Properties();

		props.put("bootstrap.servers", "localhost:9092");

		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<String, String>(props);
		String test = "<syslog-message-log source-address=\"127.0.0.1\" source-port=\"1514\" system-id=\"99\" location=\"PavanMac\">\n"
				+ "   <messages timestamp=\""
				+ iso8601OffsetString(new Date(0), ZoneId.systemDefault(), ChronoUnit.SECONDS)
				+ "\">PDMxPm1haW46IDIwMTctMTAtMDMgbG9jYWxob3N0IGZvbyVkOiBsb2FkIHRlc3RwYXZhbiAlZCBvbiB0dHkx</messages>\n"
				+ "</syslog-message-log>";
		for (int i = 0; i < 1; i++)
			producer.send(new ProducerRecord<String, String>(topicName, test));
		System.out.println("Message sent successfully");
		producer.close();

	}

	public static String iso8601OffsetString(Date d, ZoneId zone, ChronoUnit truncateTo) {
		ZonedDateTime zdt = ((d).toInstant()).atZone(zone);
		if (truncateTo != null) {
			zdt = zdt.truncatedTo(truncateTo);
		}
		return zdt.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
	}

	public static String stripExtraQuotes(String string) {
		return string.replaceAll("^\"(.*)\"$", "$1");
	}
}