package org.vertx.kafka.util;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;

import org.opennms.netmgt.syslogd.api.SyslogMessageDTO;
import org.opennms.netmgt.syslogd.api.SyslogMessageLogDTO;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import io.vertx.core.json.JsonObject;

public class HashMapCodec implements MessageCodec<HashMap, HashMap> {

	private final String KEY = "key";
	private final String VALUE = "value";

	@Override
	public void encodeToWire(Buffer buffer, HashMap s) {
		JsonObject jsonToEncode = new JsonObject();
		jsonToEncode.put(KEY, s.keySet());
		jsonToEncode.put(VALUE, s.values());

		String jsonToStr = jsonToEncode.encode();
		int length = jsonToStr.getBytes().length;
		buffer.appendInt(length);
		buffer.appendString(jsonToStr);

	}

	@Override
	public HashMap decodeFromWire(int pos, Buffer buffer) {

		int _pos = pos;
		int length = buffer.getInt(_pos);
		String jsonStr = buffer.getString(_pos += 4, _pos += length);
		JsonObject contentJson = new JsonObject(jsonStr);
		HashMap<String, String> hashMap = new HashMap<>();
		hashMap.put(contentJson.getString(KEY), contentJson.getString(VALUE));
		return hashMap;

	}

	@Override
	public HashMap transform(HashMap s) {
		return new HashMap<String, String>();
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
