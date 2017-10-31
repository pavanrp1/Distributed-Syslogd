package org.vertx.kafka;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Properties;

//import KafkaProducer packages
import org.apache.kafka.clients.producer.KafkaProducer;
//import simple producer packages
import org.apache.kafka.clients.producer.Producer;
//import ProducerRecord packages
import org.apache.kafka.clients.producer.ProducerRecord;

public class SimpleProducer2 {

	public static void main(String[] args) throws Exception {

		String topicName = "testGroup";
		
		 topicName = "syslogd";

		Properties props = new Properties();

		props.put("bootstrap.servers", "localhost:9092");

		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<String, String>(props);
		String test = "<syslog-message-log source-address=\"10.182.241.167\" source-port=\"1514\" system-id=\"99\" location=\"PavanMac\">\n"
				+ "   <messages timestamp=\""
				+ iso8601OffsetString(new Date(0), ZoneId.systemDefault(), ChronoUnit.SECONDS)
				+ "\">PDMxPm1haW46IDIwMTctMTAtMDMgbG9jYWxob3N0IGZvbyVkOiBsb2FkIHRlc3RwYXZhbiAlZCBvbiB0dHkx</messages>\n"
				+ "</syslog-message-log>";
		for (int i = 0; i < 10000; i++)
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