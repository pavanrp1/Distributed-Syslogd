package org.vertx.kafka.util;

import java.net.InetAddress;
import java.util.List;

import org.opennms.netmgt.syslogd.api.SyslogMessageDTO;
import org.opennms.netmgt.syslogd.api.SyslogMessageLogDTO;
import org.opennms.netmgt.xml.event.Log;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import io.vertx.core.json.JsonObject;

public class SyslogdDTOMessageCodec implements MessageCodec<SyslogMessageLogDTO, SyslogMessageLogDTO> {

	private final String LOCATION = "location";
	private final String SYSTEM_ID = "systemId";
	private final String SOURCE_ADDRESS = "source-address";
	private final String SOURCE_PORT = "source-port";
	private final String MESSAGE = "message";

	@Override
	public void encodeToWire(Buffer buffer, SyslogMessageLogDTO syslogMessageDTO) {
		JsonObject jsonToEncode = new JsonObject();
		jsonToEncode.put(LOCATION, syslogMessageDTO.getLocation());
		jsonToEncode.put(SYSTEM_ID, syslogMessageDTO.getSystemId());
		jsonToEncode.put(SOURCE_ADDRESS, syslogMessageDTO.getSourceAddress().toString());
		jsonToEncode.put(SOURCE_PORT, syslogMessageDTO.getSourcePort());
		jsonToEncode.put(MESSAGE, syslogMessageDTO.getMessages());

		String jsonToStr = jsonToEncode.encode();
		int length = jsonToStr.getBytes().length;
		buffer.appendInt(length);
		buffer.appendString(jsonToStr);
	}

	@Override
	public SyslogMessageLogDTO decodeFromWire(int position, Buffer buffer) {
		int _pos = position;
		int length = buffer.getInt(_pos);
		String jsonStr = buffer.getString(_pos += 4, _pos += length);
		JsonObject contentJson = new JsonObject(jsonStr);
		SyslogMessageLogDTO syslogMessageDTO = new SyslogMessageLogDTO();
		syslogMessageDTO.setLocation((String) contentJson.getValue(LOCATION));
		syslogMessageDTO.setSystemId((String) contentJson.getValue(SYSTEM_ID));
		syslogMessageDTO.setSourceAddress((InetAddress) contentJson.getValue(SOURCE_ADDRESS));
		syslogMessageDTO.setSourcePort((int) contentJson.getValue(SOURCE_PORT));
		syslogMessageDTO.setMessages((SyslogMessageDTO) contentJson.getValue(MESSAGE));
		return syslogMessageDTO;
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