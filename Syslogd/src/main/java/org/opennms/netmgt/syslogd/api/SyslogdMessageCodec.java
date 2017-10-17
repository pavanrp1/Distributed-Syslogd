package org.opennms.netmgt.syslogd.api;

import org.opennms.netmgt.xml.event.Log;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import io.vertx.core.json.JsonObject;

public class SyslogdMessageCodec implements MessageCodec<Log, Log> {

	private final String EVENTLOG = "eventlog";

	@Override
	public void encodeToWire(Buffer buffer, Log eventLogMessage) {
		JsonObject jsonToEncode = new JsonObject();
		jsonToEncode.put(EVENTLOG, eventLogMessage.getEvents());
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
		Log eventLog = (Log) contentJson.getValue(EVENTLOG);
		return eventLog;
	}

	@Override
	public Log transform(Log eventLogMessage) {
		return eventLogMessage;
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