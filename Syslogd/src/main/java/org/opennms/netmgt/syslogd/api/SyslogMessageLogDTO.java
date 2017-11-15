/*******************************************************************************
 * This file is part of OpenNMS(R).
 *
 * Copyright (C) 2016-2016 The OpenNMS Group, Inc.
 * OpenNMS(R) is Copyright (C) 1999-2016 The OpenNMS Group, Inc.
 *
 * OpenNMS(R) is a registered trademark of The OpenNMS Group, Inc.
 *
 * OpenNMS(R) is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 *
 * OpenNMS(R) is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with OpenNMS(R).  If not, see:
 *      http://www.gnu.org/licenses/
 *
 * For more information contact:
 *     OpenNMS(R) Licensing <license@opennms.org>
 *     http://www.opennms.org/
 *     http://www.opennms.com/
 *******************************************************************************/

package org.opennms.netmgt.syslogd.api;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import org.opennms.core.ipc.sink.api.Message;
import org.opennms.core.network.InetAddressXmlAdapter;
import org.opennms.netmgt.config.SyslogdConfig;
import org.opennms.netmgt.model.events.EventBuilder;
import org.opennms.netmgt.syslogd.SyslogMessage;

import com.google.gson.Gson;
import com.sun.xml.bind.v2.runtime.RuntimeUtil.ToStringAdapter;

import java.util.Objects;

@XmlRootElement(name = "syslog-message-log")
@XmlAccessorType(XmlAccessType.FIELD)
public class SyslogMessageLogDTO implements Message {
	@XmlAttribute(name = "source-address")
	@XmlJavaTypeAdapter(InetAddressXmlAdapter.class)
	private InetAddress sourceAddress;
	@XmlAttribute(name = "source-port")
	private int sourcePort;
	@XmlAttribute(name = "system-id")
	private String systemId;
	@XmlAttribute(name = "location")
	private String location;
	@XmlElement(name = "messages")
	private SyslogMessageDTO messages;

	private EventBuilder eventBuilder;

	private SyslogMessage syslogMessage;

	public SyslogMessage getSyslogMessage() {
		return syslogMessage;
	}

	public void setSyslogMessage(SyslogMessage syslogMessage) {
		this.syslogMessage = syslogMessage;
	}

	public EventBuilder getEventBuilder() {
		return eventBuilder;
	}

	public void setEventBuilder(EventBuilder eventBuilder) {
		this.eventBuilder = eventBuilder;
	}

	private SyslogdConfig syslogdConfig;

	public SyslogdConfig getSyslogdConfig() {
		return syslogdConfig;
	}

	public void setSyslogdConfig(SyslogdConfig syslogdConfig) {
		this.syslogdConfig = syslogdConfig;
	}

	private Map<String, String> paramsMap;

	public Map<String, String> getParamsMap() {
		return paramsMap;
	}

	public void setParamsMap(Map<String, String> paramsMap) {
		this.paramsMap = paramsMap;
	}

	public SyslogMessageLogDTO() {
		messages = new SyslogMessageDTO();
	}

	public SyslogMessageLogDTO(String location, String systemId, InetSocketAddress source) {
		this(location, systemId, source, new SyslogMessageDTO());
	}

	public SyslogMessageLogDTO(String location, String systemId, InetSocketAddress source, SyslogMessageDTO messages) {
		this.location = location;
		this.systemId = systemId;
		this.sourceAddress = source.getAddress();
		this.sourcePort = source.getPort();
		this.messages = messages;
	}

	public InetAddress getSourceAddress() {
		return sourceAddress;
	}

	public void setSourceAddress(InetAddress sourceAddress) {
		this.sourceAddress = sourceAddress;
	}

	public int getSourcePort() {
		return sourcePort;
	}

	public void setSourcePort(int sourcePort) {
		this.sourcePort = sourcePort;
	}

	public String getSystemId() {
		return systemId;
	}

	public void setSystemId(String systemId) {
		this.systemId = systemId;
	}

	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	public SyslogMessageDTO getMessages() {
		return messages;
	}

	public void setMessages(SyslogMessageDTO messages) {
		this.messages = messages;
	}

	@Override
	public boolean equals(final Object other) {
		if (!(other instanceof SyslogMessageLogDTO)) {
			return false;
		}
		SyslogMessageLogDTO castOther = (SyslogMessageLogDTO) other;
		return Objects.equals(sourceAddress, castOther.sourceAddress)
				&& Objects.equals(sourcePort, castOther.sourcePort) && Objects.equals(systemId, castOther.systemId)
				&& Objects.equals(location, castOther.location) && Objects.equals(messages, castOther.messages);
	}

	@Override
	public int hashCode() {
		return Objects.hash(sourceAddress, sourcePort, systemId, location, messages);
	}
}
