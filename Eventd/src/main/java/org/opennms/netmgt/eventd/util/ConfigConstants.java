package org.opennms.netmgt.eventd.util;

public class ConfigConstants {

	public static final String KAFKA_CONSUMER_ADDRESS = "kafka.message.consumer.address";
	public static final String PARAMS_CONSUMER_ADDRESS = "params.message.consumer.address";
	public static final String CONVERT_TO_EVENT_CONSUMER_ADDRESS = "converttoevent.message.consumer.address";
	public static final String DEFAULT_TO_EVENT_CONSUMER_ADDRESS = "defaulteventipc.message.consumer.address";
	public static final String EVENTEXPANDER_TO_EVENT_CONSUMER_ADDRESS = "eventexpander.message.consumer.address";
	public static final String HIBERNATE_TO_EVENT_CONSUMER_ADDRESS = "hibernatewriter.message.consumer.address";
	public static final String EVENTBROADCASTER_TO_EVENT_CONSUMER_ADDRESS = "eventbroadcaster.message.consumer.address";

	public static final String TOPICS = "topics";
	public static final String GROUP_ID = "group.id";
	public static final String BACKOFF_INCREMENT_MS = "backoff.increment.ms";
	public static final String AUTO_OFFSET_RESET = "autooffset.reset";
	public static final String EVENTBUS_ADDRESS = "eventbus.address";
	public static final String CONSUMER_POLL_INTERVAL_MS = "consumer.poll.interval.ms";
	public static final String ZK_CONNECT = "zookeeper.connect";
	public static final String KEY_DESERIALIZER_CLASS = "key.deserializer";
	public static final String VALUE_DESERIALIZER_CLASS = "value.deserializer";
	public static final String DEFAULT_DESERIALIZER_CLASS = "org.apache.kafka.common.serialization.StringDeserializer";
	public static final String CONSUMER_ERROR_TOPIC = "kafka.consumer.error";

	// common
	public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";

	// producer constants
	public static final String PRODUCER_ERROR_TOPIC = "kafka.producer.error";
	public static final String DEFAULT_TOPIC = "default.topic";
	public static final String KEY_SERIALIZER_CLASS = "key.serializer";
	public static final String PRODUCER_TYPE = "producer.type";
	public static final String SERIALIZER_CLASS = "serializer.class";
	public static final String VALUE_SERIALIZER_CLASS = "value.serializer";
	public static final String DEFAULT_SERIALIZER_CLASS = "org.apache.kafka.common.serialization.StringSerializer";
	public static final String MAX_BLOCK_MS = "max.block.ms";

	// event bus fields
	public static final String TOPIC_FIELD = "topic";
	public static final String KEY_FIELD = "key";
	public static final String VALUE_FIELD = "value";
	public static final String PARTITION_FIELD = "partition";
	public static final String OPENNMS_HOME = "opennms.home";

	public static final String LOCALHOST = "127.0.0.1";

}
