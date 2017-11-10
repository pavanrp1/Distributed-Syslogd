package org.opennms.netmgt.syslogd.api;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import io.vertx.core.json.JsonObject;

public class Builder implements MessageCodec<StringBuilder, StringBuilder> {

	private final String LOCATION = "location";
	private final String SYSTEM_ID = "systemId";
	private final String SOURCE_ADDRESS = "source-address";
	private final String SOURCE_PORT = "source-port";
	private final String MESSAGE = "message";
	private final String CONFIG = "config";
	private final String PARAM = "param";

	@Override
	public void encodeToWire(Buffer buffer, StringBuilder s) {
		// TODO Auto-generated method stub

		JsonObject jsonToEncode = new JsonObject();
		jsonToEncode.put(MESSAGE, s);

		String jsonToStr = jsonToEncode.encode();
		int length = jsonToStr.getBytes().length;
		buffer.appendInt(length);
		buffer.appendString(jsonToStr);

	}

	@Override
	public StringBuilder decodeFromWire(int pos, Buffer buffer) {
		int _pos = pos;
		int length = buffer.getInt(_pos);
		String jsonStr = buffer.getString(_pos += 4, _pos += length);
		JsonObject contentJson = new JsonObject(jsonStr);
		StringBuilder builder = new StringBuilder();
		builder.append(contentJson.getValue(MESSAGE));
		return builder;
	}

	@Override
	public StringBuilder transform(StringBuilder s) {
		return s;
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
