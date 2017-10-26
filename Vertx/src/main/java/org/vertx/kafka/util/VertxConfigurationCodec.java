package org.vertx.kafka.util;

import java.util.List;

import org.opennms.core.ipc.sink.api.MessageConsumerManager;
import org.opennms.netmgt.config.SyslogdConfig;
import org.opennms.netmgt.dao.api.DistPollerDao;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import io.vertx.core.json.JsonObject;

public class VertxConfigurationCodec implements MessageCodec<VertxConfiguration, VertxConfiguration> {

	private final String DISTPOLLER = "dispoller";
	private final String KAFKA_CONSUMER = "kafkaconsumer";
	private final String MESSAGE_CONSUMER_MANAGER = "messageconsumermanager";
	private final String SYSLOGD_CONFIG = "syslogdconfig";
	private final String GROKPATTERN_LIST = "grokpatternlist";

	@Override
	public void encodeToWire(Buffer buffer, VertxConfiguration vertxConfiguration) {
		JsonObject jsonToEncode = new JsonObject();
		jsonToEncode.put(DISTPOLLER, vertxConfiguration.getDistPollerDao());
		jsonToEncode.put(KAFKA_CONSUMER, vertxConfiguration.getKafkaConsumerConfig());
		jsonToEncode.put(MESSAGE_CONSUMER_MANAGER, vertxConfiguration.getMessageConsumerManager());
		jsonToEncode.put(SYSLOGD_CONFIG, vertxConfiguration.getSyslogdConfig());
		jsonToEncode.put(GROKPATTERN_LIST, vertxConfiguration.getGrokPatternsList());

		String jsonToStr = jsonToEncode.encode();
		int length = jsonToStr.getBytes().length;
		buffer.appendInt(length);
		buffer.appendString(jsonToStr);
	}

	@Override
	public VertxConfiguration decodeFromWire(int pos, Buffer buffer) {
		int _pos = pos;
		int length = buffer.getInt(_pos);
		String jsonStr = buffer.getString(_pos += 4, _pos += length);
		JsonObject contentJson = new JsonObject(jsonStr);
		VertxConfiguration vertxConfiguration = new VertxConfiguration();
		vertxConfiguration.setDistPollerDao((DistPollerDao) contentJson.getValue(DISTPOLLER));
		vertxConfiguration.setKafkaConsumerConfig((JsonObject) contentJson.getValue(KAFKA_CONSUMER));
		vertxConfiguration
				.setMessageConsumerManager((MessageConsumerManager) contentJson.getValue(MESSAGE_CONSUMER_MANAGER));
		vertxConfiguration.setSyslogdConfig((SyslogdConfig) contentJson.getValue(SYSLOGD_CONFIG));
		vertxConfiguration.setGrokPatternsList((List<String>) contentJson.getValue(GROKPATTERN_LIST));
		return vertxConfiguration;
	}

	@Override
	public VertxConfiguration transform(VertxConfiguration vertxConfiguration) {
		return vertxConfiguration;
	}

	@Override
	public String name() {
		return this.getClass().getSimpleName();
	}

	@Override
	public byte systemCodecID() {
		return -1;
	}

}
