package org.opennms.netmgt.syslogd;

public class SyslogdTestRun {

	private static ParamsLoader paramsLoader;

	private static ConvertToEvent convertToEvent;

	public static void main(String[] args) throws Exception {
		for (int i = 0; i < 1; i++) {
			paramsLoader = new ParamsLoader();
			paramsLoader.main(args);
		}
		for (int i = 0; i < 1; i++) {
			convertToEvent = new ConvertToEvent();
			convertToEvent.main(args);
		}

	}

}
