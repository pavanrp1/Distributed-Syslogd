package org.vertx.kafka.util;

import org.opennms.netmgt.xml.event.Log;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import io.vertx.core.json.JsonObject;

public class CustomMessageCodec implements MessageCodec<Log, Log> {
	@Override
	public void encodeToWire(Buffer buffer, Log customMessage) {
		JsonObject jsonToEncode = new JsonObject();
		jsonToEncode.put("eventLog", customMessage.getEvents());
		String jsonToStr = jsonToEncode.encode();
		int length = jsonToStr.getBytes().length;
		buffer.appendInt(length);
		buffer.appendString(jsonToStr);
	}

	@Override
	public Log decodeFromWire(int position, Buffer buffer) {
		int _pos = position;
		int length = buffer.getInt(_pos);
		String jsonStr = buffer.getString(_pos += 4, _pos += length);
		JsonObject contentJson = new JsonObject(jsonStr);
		Log eventLog = (Log) contentJson.getValue("eventLog");
		return eventLog;
	}

	@Override
	public Log transform(Log customMessage) {
		return customMessage;
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