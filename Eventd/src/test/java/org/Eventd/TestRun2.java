package org.Eventd;

import org.opennms.netmgt.eventd.DefaultEventHandlerImpl;

public class TestRun2 {

	private static DefaultEventHandlerImpl deafultImpl;

	public static void main(String[] args) {

		for (int i = 0; i < 5; i++) {
			deafultImpl = new DefaultEventHandlerImpl();
			deafultImpl.main(args);
		}

	}

}
