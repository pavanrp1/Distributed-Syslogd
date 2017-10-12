/**
 * 
 */
package org.vertx.kafka.util;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.opennms.netmgt.syslogd.SyslogSinkConsumer;
import org.opennms.netmgt.xml.event.Log;
import org.vertx.kafka.SimpleConsumer;

import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

/**
 * @author pk015603
 *
 */
public class KafkaEvent {

	private static final Logger logger = LoggerFactory.getLogger(KafkaEvent.class);
	public final String topic;
	public final String key;
	public final String value;
	public final int partition;

	public KafkaEvent(JsonObject event) {
		topic = event.getString(ConfigConstants.TOPIC_FIELD);
		key = event.getString(ConfigConstants.KEY_FIELD);
		value = event.getString(ConfigConstants.VALUE_FIELD);
		partition = event.getInteger(ConfigConstants.PARTITION_FIELD);
	}

	@Override
	public String toString() {
		return "KafkaEvent{" + "topic='" + topic + '\'' + ", key='" + key + '\'' + ", value='" + value + '\''
				+ ", partition=" + partition + '}';
	}

	/**
	 * Convert a Kafka ConsumerRecord into an event bus event.
	 *
	 * @param record
	 *            the Kafka record
	 * @return the record to send over the event bus
	 */
	public static Log createEventForBus(ConsumerRecord<String, String> record) {
		try {
			SyslogSinkConsumer consume = new SyslogSinkConsumer();
			consume.setSyslogdConfig(SimpleConsumer.getSylogConfig());
			consume.handleMessage(SimpleConsumer.getSyslogMessageLogDTO(record.value()));
			return consume.getEventLog();
		} catch (Exception e) {
			logger.error("Failed to consume syslogd message " + e.getMessage());
		}
		return null;

	}
}
