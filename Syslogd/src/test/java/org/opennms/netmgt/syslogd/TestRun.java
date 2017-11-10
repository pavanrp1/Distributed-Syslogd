package org.opennms.netmgt.syslogd;

import java.io.IOException;

public class TestRun {

	private static ParamsLoader loader;
	private static ConvertToEvent convertToEvent;

	public static void main(String[] args) throws IOException {
		for (int i = 0; i < 5; i++) {
			loader = new ParamsLoader();
			loader.main(args);
		}
		for (int i = 0; i < 5; i++) {
			convertToEvent = new ConvertToEvent();
			convertToEvent.main(args);
		}

	}

}
