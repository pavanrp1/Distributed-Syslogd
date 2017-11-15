/**
 * 
 */
package org.vertx.kafka;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.opennms.netmgt.syslogd.SyslogTimeStamp;
import org.opennms.netmgt.eventd.Runner;
import org.vertx.kafka.util.ConfigConstants;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

/**
 * @author pk015603
 *
 */
public class KafkaMessageConsumer extends AbstractVerticle {

	private static final Logger logger = LoggerFactory.getLogger(KafkaMessageConsumer.class);

	private static final int DEFAULT_POLL_MS = 100;

	private String busAddress;

	private EventBus kafkaEventBus;

	private AtomicBoolean concurrentRunning;

	private KafkaConsumer kafkaConsumer;

	private static List<String> topics;

	private static JsonObject verticleConfig;

	private ExecutorService backgroundConsumer;

	private AtomicInteger eventCount = new AtomicInteger();

	private int pollIntervalMs = 10;

	public KafkaMessageConsumer() {
	}

	public EventBus getBus() {
		return kafkaEventBus;
	}

	public void setBus(EventBus bus) {
		this.kafkaEventBus = bus;
	}

	public static void main(String[] args) {
		System.setProperty("opennms.home", "src/test/resources");
		// System.setProperty("org.opennms.core.test.mockLogger.defaultLogLevel",
		// "WARN");
		verticleConfig = new JsonObject();
		verticleConfig.put(ConfigConstants.GROUP_ID, "syslogd");
		verticleConfig.put(ConfigConstants.ZK_CONNECT, "localhost:2181");
		verticleConfig.put(ConfigConstants.BOOTSTRAP_SERVERS, "localhost:9092");
		topics = new ArrayList<String>();
		topics.add("syslogd");
		verticleConfig.put("topics", new JsonArray(topics));
		DeploymentOptions deployment = new DeploymentOptions();
		deployment.setWorker(true);
		deployment.setWorkerPoolSize(Integer.MAX_VALUE);
		deployment.setMultiThreaded(true);
		Runner.runClusteredExample1(KafkaMessageConsumer.class, deployment);
	}

	@Override
	public void start() {
		try {

			// creating event bus at the startup
			kafkaEventBus = vertx.eventBus();

			concurrentRunning = new AtomicBoolean(true);

			// creating kafka configuration and properties
			Properties kafkaConfig = populateKafkaConfig(verticleConfig);
			JsonArray topicConfig = verticleConfig.getJsonArray(ConfigConstants.TOPICS);

			busAddress = verticleConfig.getString(ConfigConstants.EVENTBUS_ADDRESS, "syslogd.message.consumer");
			pollIntervalMs = verticleConfig.getInteger(ConfigConstants.CONSUMER_POLL_INTERVAL_MS, DEFAULT_POLL_MS);

			Runtime.getRuntime().addShutdownHook(new Thread() {
				public void run() {
					shutdown();
				}
			});

			backgroundConsumer = Executors.newSingleThreadExecutor();
			backgroundConsumer.submit(() -> {
				kafkaConsumer = new KafkaConsumer(kafkaConfig);
				topics = new ArrayList<>();
				for (int i = 0; i < topicConfig.size(); i++) {
					topics.add(topicConfig.getString(i));
					logger.info("Subscribing to topic ");
				}

				consumeFromKafka();
			});

		} catch (Exception ex) {
			String error = "Failed to startup";
			logger.error(error, ex);
			kafkaEventBus.publish(ConfigConstants.CONSUMER_ERROR_TOPIC,
					getErrorString("Failed to startup", ex.getMessage()));
		}
	}

	private String getErrorString(String error, String errorMessage) {
		return String.format("%s - error: %s", error, errorMessage);
	}

	/**
	 * Handles looping and consuming
	 */
	private void consumeFromKafka() {
		kafkaConsumer.subscribe(topics);
		while (concurrentRunning.get()) {
			try {
				ConsumerRecords records = kafkaConsumer.poll(pollIntervalMs);

				if (records == null) {
					continue;
				}

				Iterator<ConsumerRecord<String, String>> recordIterator = records.iterator();

				while (recordIterator.hasNext()) {
					ConsumerRecord<String, String> consumerRecord = recordIterator.next();
					sendConsumedMessage(consumerRecord);
				}
			} catch (Exception ex) {
				String error = "Error consuming messages from kafka";
				logger.error(error, ex);
				kafkaEventBus.send(ConfigConstants.CONSUMER_ERROR_TOPIC, getErrorString(error, ex.getMessage()));
			}
		}
	}

	@Override
	public void stop() {
		concurrentRunning.compareAndSet(true, false);
	}

	/**
	 * Send the inbound message to the event bus consumer.
	 *
	 * @param record
	 *            the kafka event
	 */
	private void sendConsumedMessage(ConsumerRecord<String, String> record) {
		try {
			kafkaEventBus.send("syslogd.message.consumer", record.value());

			// vertx.executeBlocking(f -> {
			// vertx.eventBus().send("syslogd.message.consumer", record.value());
			// f.complete();
			// }, false, r -> {
			// });

		} catch (Exception ex) {
			String error = String.format("Error sending messages on event bus - record: %s", record.toString());
			logger.error(error, ex);
			kafkaEventBus.send(ConfigConstants.CONSUMER_ERROR_TOPIC, getErrorString(error, ex.getMessage()));
		}
	}

	private Properties populateKafkaConfig(JsonObject config) {
		Properties consumerConfig = new Properties();
		consumerConfig.put(ConfigConstants.ZK_CONNECT, config.getString(ConfigConstants.ZK_CONNECT, "localhost:2181"));
		consumerConfig.put(ConfigConstants.BACKOFF_INCREMENT_MS,
				config.getString(ConfigConstants.BACKOFF_INCREMENT_MS, "100"));
		consumerConfig.put(ConfigConstants.AUTO_OFFSET_RESET,
				config.getString(ConfigConstants.AUTO_OFFSET_RESET, "smallest"));

		consumerConfig.put(ConfigConstants.BOOTSTRAP_SERVERS, getRequiredConfig(ConfigConstants.BOOTSTRAP_SERVERS));

		consumerConfig.put(ConfigConstants.KEY_DESERIALIZER_CLASS,
				config.getString(ConfigConstants.KEY_DESERIALIZER_CLASS, ConfigConstants.DEFAULT_DESERIALIZER_CLASS));
		consumerConfig.put(ConfigConstants.VALUE_DESERIALIZER_CLASS,
				config.getString(ConfigConstants.VALUE_DESERIALIZER_CLASS, ConfigConstants.DEFAULT_DESERIALIZER_CLASS));
		consumerConfig.put(ConfigConstants.GROUP_ID, getRequiredConfig(ConfigConstants.GROUP_ID));
		return consumerConfig;
	}

	private String getRequiredConfig(String key) {
		String value = verticleConfig.getString(key, null);

		if (null == value) {
			throw new IllegalArgumentException(String.format("Required config value not found key: %s", key));
		}
		return value;
	}

	/**
	 * Handle stopping the consumer.
	 */
	private void shutdown() {
		concurrentRunning.compareAndSet(true, false);
		try {
			if (kafkaConsumer != null) {
				try {
					kafkaConsumer.unsubscribe();
					kafkaConsumer.close();
					kafkaConsumer = null;
				} catch (Exception ex) {
				}
			}

			if (backgroundConsumer != null) {
				backgroundConsumer.shutdown();
				backgroundConsumer = null;
			}
		} catch (Exception ex) {
			logger.error("Failed to close consumer", ex);
		}
	}

	public JsonObject getVerticleConfig() {
		return verticleConfig;
	}

	public void setVerticleConfig(JsonObject verticleConfig) {
		this.verticleConfig = verticleConfig;
	}

}
