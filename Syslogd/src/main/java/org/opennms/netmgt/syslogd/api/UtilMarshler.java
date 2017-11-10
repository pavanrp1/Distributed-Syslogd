package org.opennms.netmgt.syslogd.api;

import org.opennms.core.xml.XmlHandler;

public class UtilMarshler extends XmlHandler<SyslogMessageLogDTO> {

	public UtilMarshler(Class<SyslogMessageLogDTO> clazz) {
		super(clazz);
	}

	@Override
	public synchronized String marshal(SyslogMessageLogDTO obj) {
		return super.marshal(obj);
	}

	@Override
	public synchronized SyslogMessageLogDTO unmarshal(String xml) {
		return super.unmarshal(xml);
	}

}
