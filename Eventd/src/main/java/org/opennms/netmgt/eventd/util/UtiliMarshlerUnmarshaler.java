package org.opennms.netmgt.eventd.util;

import org.opennms.core.xml.XmlHandler;

public class UtiliMarshlerUnmarshaler extends XmlHandler<Object> {

	public UtiliMarshlerUnmarshaler(Class clazz) {
		super(clazz);
	}

	@Override
	public synchronized String marshal(Object obj) {
		return super.marshal(obj);
	}

	@Override
	public synchronized Object unmarshal(String xml) {
		return super.unmarshal(xml);
	}

}
