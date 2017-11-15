package org.opennms.netmgt.syslogd;

import java.io.IOException;

import org.opennms.netmgt.eventd.DefaultEventHandlerImpl;

public class TestRun {

	private static ParamsLoader loader;
	private static ConvertToEvent convertToEvent;
	private static DefaultEventHandlerImpl deafultImpl;

	public static void main(String[] args) throws IOException {
		for (int i = 0; i < 1; i++) {
			loader = new ParamsLoader();
			loader.main(args);
		}
		for (int i = 0; i < 2; i++) {
			convertToEvent = new ConvertToEvent();
			convertToEvent.main(args);
		}

	}

}
