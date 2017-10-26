package org.vertx.kafka.util;

import java.util.List;

import org.opennms.core.ipc.sink.api.MessageConsumerManager;
import org.opennms.netmgt.config.SyslogdConfig;
import org.opennms.netmgt.dao.api.DistPollerDao;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;

public class VertxConfiguration {

	private MessageConsumerManager messageConsumerManager;

	private SyslogdConfig syslogdConfig;

	private DistPollerDao distPollerDao;

	private static List<String> grokPatternsList;

	private JsonObject kafkaConsumerConfig;

	private EventBus configurationEventBus;

	public JsonObject getKafkaConsumerConfig() {
		return kafkaConsumerConfig;
	}

	public void setKafkaConsumerConfig(JsonObject kafkaConsumerConfig) {
		this.kafkaConsumerConfig = kafkaConsumerConfig;
	}

	public void setGrokPatternsList(List<String> grokPatternsList) {
		VertxConfiguration.grokPatternsList = grokPatternsList;
	}

	public List<String> getGrokPatternsList() {
		return grokPatternsList;
	}

	public MessageConsumerManager getMessageConsumerManager() {
		return messageConsumerManager;
	}

	public void setMessageConsumerManager(MessageConsumerManager messageConsumerManager) {
		this.messageConsumerManager = messageConsumerManager;
	}

	public SyslogdConfig getSyslogdConfig() {
		return syslogdConfig;
	}

	public void setSyslogdConfig(SyslogdConfig syslogdConfig) {
		this.syslogdConfig = syslogdConfig;
	}

	public DistPollerDao getDistPollerDao() {
		return distPollerDao;
	}

	public void setDistPollerDao(DistPollerDao distPollerDao) {
		this.distPollerDao = distPollerDao;
	}

}
