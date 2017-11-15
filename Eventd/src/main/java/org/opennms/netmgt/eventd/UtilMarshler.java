package org.opennms.netmgt.eventd;

import org.opennms.core.xml.XmlHandler;

public class UtilMarshler extends XmlHandler<Object> {

	public UtilMarshler(Class clazz) {
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
