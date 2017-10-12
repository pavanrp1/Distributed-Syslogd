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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.opennms.core.ipc.sink.api.MessageConsumer;
import org.opennms.netmgt.config.SyslogdConfigFactory;
import org.opennms.netmgt.syslogd.SyslogSinkConsumer;
import org.opennms.netmgt.syslogd.api.SyslogConnection;
import org.opennms.netmgt.syslogd.api.SyslogMessageLogDTO;
import org.opennms.netmgt.xml.event.Log;
import org.vertx.kafka.util.ConfigConstants;
import org.vertx.kafka.util.CustomMessageCodec;
import org.vertx.kafka.util.KafkaEvent;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

/**
 * @author pk015603
 *
 */
public class SimpleConsumer extends AbstractVerticle {

	public static final String EVENTBUS_DEFAULT_ADDRESS = "kafka.message.consumer";
	public static final int DEFAULT_POLL_MS = 100;
	private String busAddress;
	private static final Logger logger = LoggerFactory.getLogger(SimpleConsumer.class);
	private EventBus bus;

	public EventBus getBus() {
		return bus;
	}

	public void setBus(EventBus bus) {
		this.bus = bus;
	}

	private AtomicBoolean running;
	private KafkaConsumer consumer;
	private List<String> topics;
	private JsonObject verticleConfig;

	private static SyslogdConfigFactory sylogConfig;

	private static MessageConsumer<SyslogConnection, SyslogMessageLogDTO> messageConsumer;

	private ExecutorService backgroundConsumer;
	private int pollIntervalMs = 10;

	private boolean isTrue = true;
	
	public static boolean isFinished=false;

	public SimpleConsumer(Vertx vertx) {
		this.vertx = vertx;
	}

	@Override
	public void start(final Future<Void> startedResult) {
		try {
			bus = vertx.eventBus();
			if (isTrue) {
				bus.registerDefaultCodec(Log.class, new CustomMessageCodec());
				isTrue = false;
			}
			running = new AtomicBoolean(true);
			Properties kafkaConfig = populateKafkaConfig(verticleConfig);
			JsonArray topicConfig = verticleConfig.getJsonArray(ConfigConstants.TOPICS);

			busAddress = verticleConfig.getString(ConfigConstants.EVENTBUS_ADDRESS, EVENTBUS_DEFAULT_ADDRESS);
			pollIntervalMs = verticleConfig.getInteger(ConfigConstants.CONSUMER_POLL_INTERVAL_MS, DEFAULT_POLL_MS);

			Runtime.getRuntime().addShutdownHook(new Thread() {
				// try to disconnect from ZK as gracefully as possible
				public void run() {
					shutdown();
				}
			});

			backgroundConsumer = Executors.newSingleThreadExecutor();
			backgroundConsumer.submit(() -> {
				try {
					consumer = new KafkaConsumer(kafkaConfig);
					topics = new ArrayList<>();
					for (int i = 0; i < topicConfig.size(); i++) {
						topics.add(topicConfig.getString(i));
						logger.info("Subscribing to topic ");
					}

					// signal success before we enter read loop
					startedResult.complete();
					consume();
				} catch (Exception ex) {
					String error = "Failed to startup";
					logger.error(error, ex);
					bus.send(ConfigConstants.CONSUMER_ERROR_TOPIC, getErrorString(error, ex.getMessage()));
					startedResult.fail(ex);
				}
			});
			
		} catch (Exception ex) {
			String error = "Failed to startup";
			logger.error(error, ex);
			bus.send(ConfigConstants.CONSUMER_ERROR_TOPIC, getErrorString("Failed to startup", ex.getMessage()));
			startedResult.fail(ex);
		}
	}

	private String getErrorString(String error, String errorMessage) {
		return String.format("%s - error: %s", error, errorMessage);
	}

	/**
	 * Handles looping and consuming
	 */
	private void consume() {
		consumer.subscribe(topics);
		while (running.get()) {
			try {
				ConsumerRecords records = consumer.poll(pollIntervalMs);

				// there were no messages
				if (records == null) {
					continue;
				}

				Iterator<ConsumerRecord<String, String>> iterator = records.iterator();

				while (iterator.hasNext()) {
					ConsumerRecord<String, String> test = iterator.next();
					sendMessage(test);
				}
			} catch (Exception ex) {
				String error = "Error consuming messages from kafka";
				logger.error(error, ex);
				bus.send(ConfigConstants.CONSUMER_ERROR_TOPIC, getErrorString(error, ex.getMessage()));
			}
		}
	}

	@Override
	public void stop() {
		running.compareAndSet(true, false);
	}

	/**
	 * Send the inbound message to the event bus consumer.
	 *
	 * @param record
	 *            the kafka event
	 */
	private void sendMessage(ConsumerRecord<String, String> record) {
		try {
			Log eventLog = KafkaEvent.createEventForBus(record);
			bus.send(busAddress, eventLog);
			System.out.println("Event number recieved " + SyslogSinkConsumer.eventCount);
		} catch (Exception ex) {
			String error = String.format("Error sending messages on event bus - record: %s", record.toString());
			logger.error(error, ex);
			bus.send(ConfigConstants.CONSUMER_ERROR_TOPIC, getErrorString(error, ex.getMessage()));
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
		running.compareAndSet(true, false);
		try {
			if (consumer != null) {
				try {
					consumer.unsubscribe();
					consumer.close();
					consumer = null;
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

	public static synchronized SyslogMessageLogDTO getSyslogMessageLogDTO(String message) {
		try {
			synchronized (message) {
				return messageConsumer.getModule().unmarshal(message);
			}
		} catch (Exception e) {
			logger.error("Unable to load syslogmessagelogdto " + e.getMessage());
			return null;
		}
	}

	public MessageConsumer<SyslogConnection, SyslogMessageLogDTO> getMessageConsumer() {
		return messageConsumer;
	}

	public void setMessageConsumer(MessageConsumer<SyslogConnection, SyslogMessageLogDTO> messageConsumer) {
		this.messageConsumer = messageConsumer;
	}

	public static SyslogdConfigFactory getSylogConfig() {
		return sylogConfig;
	}

	public void setSylogConfig(SyslogdConfigFactory sylogConfig) {
		this.sylogConfig = sylogConfig;
	}

}
