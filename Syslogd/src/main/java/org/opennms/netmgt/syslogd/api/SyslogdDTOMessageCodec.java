package org.opennms.netmgt.syslogd.api;

import java.lang.reflect.Type;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.opennms.netmgt.config.SyslogdConfig;
import org.opennms.netmgt.config.SyslogdConfigFactory;
import org.opennms.netmgt.syslogd.ByteBufferXmlAdapter;
import org.opennms.netmgt.syslogd.api.SyslogMessageDTO;
import org.opennms.netmgt.syslogd.api.SyslogMessageLogDTO;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import io.vertx.core.json.JsonObject;

public class SyslogdDTOMessageCodec implements MessageCodec<SyslogMessageLogDTO, SyslogMessageLogDTO> {

	private final String LOCATION = "location";
	private final String SYSTEM_ID = "systemId";
	private final String SOURCE_ADDRESS = "source-address";
	private final String SOURCE_PORT = "source-port";
	private final String MESSAGE = "message";
	private final String CONFIG = "config";
	private final String PARAM = "param";
	private Gson objectToGson;

	@Override
	public void encodeToWire(Buffer buffer, SyslogMessageLogDTO syslogMessageDTO) {
		objectToGson = new Gson();
		JsonObject jsonToEncode = new JsonObject();
		jsonToEncode.put(LOCATION, syslogMessageDTO.getLocation());
		jsonToEncode.put(SYSTEM_ID, syslogMessageDTO.getSystemId());
		jsonToEncode.put(SOURCE_ADDRESS, objectToGson.toJson(syslogMessageDTO.getSourceAddress()));
		jsonToEncode.put(SOURCE_PORT, syslogMessageDTO.getSourcePort());
		jsonToEncode.put(MESSAGE, objectToGson.toJson(syslogMessageDTO.getMessages()));
		jsonToEncode.put(CONFIG, objectToGson.toJson(syslogMessageDTO.getSyslogdConfig()));
		jsonToEncode.put(PARAM, objectToGson.toJson(syslogMessageDTO.getParamsMap()));

		String jsonToStr = jsonToEncode.encode();
		int length = jsonToStr.getBytes().length;
		buffer.appendInt(length);
		buffer.appendString(jsonToStr);
	}

	@Override
	public SyslogMessageLogDTO decodeFromWire(int position, Buffer buffer) {
		try {
			// objectToGson = new GsonBuilder().registerTypeAdapter(ByteBuffer.class, new
			// ByteBufferXmlAdapter()).create();
			objectToGson = new GsonBuilder().registerTypeAdapter(ByteBuffer.class, new ByteBufferXmlAdapter()).create();
			Type mapType = new TypeToken<Map<String, String>>() {
			}.getType();

			int _pos = position;
			int length = buffer.getInt(_pos);
			String jsonStr = buffer.getString(_pos += 4, _pos += length);
			JsonObject contentJson = new JsonObject(jsonStr);
			SyslogMessageLogDTO syslogMessageDTO = new SyslogMessageLogDTO();
			syslogMessageDTO.setLocation((String) contentJson.getValue(LOCATION));
			syslogMessageDTO.setSystemId((String) contentJson.getValue(SYSTEM_ID));
			syslogMessageDTO.setSourceAddress(
					objectToGson.fromJson((String) contentJson.getValue(SOURCE_ADDRESS), InetAddress.class));
			syslogMessageDTO.setSourcePort((int) contentJson.getValue(SOURCE_PORT));
			syslogMessageDTO
					.setMessages(objectToGson.fromJson((String) contentJson.getValue(MESSAGE), SyslogMessageDTO.class));
			syslogMessageDTO.setParamsMap(objectToGson.fromJson((String) contentJson.getValue(PARAM), mapType));
			syslogMessageDTO.setSyslogdConfig(
					objectToGson.fromJson((String) contentJson.getValue(CONFIG), SyslogdConfigFactory.class));
			return syslogMessageDTO;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public SyslogMessageLogDTO transform(SyslogMessageLogDTO syslogMessageDTO) {
		return syslogMessageDTO;
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