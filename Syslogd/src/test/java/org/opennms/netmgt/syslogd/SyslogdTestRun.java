package org.opennms.netmgt.syslogd;

public class SyslogdTestRun {

	public static void main(String[] args) throws Exception {
		for (int i = 0; i < 1; i++) {
			ParamsLoader.main(args);
		}
		for (int i = 0; i < 2; i++) {
			ConvertToEvent.main(args);
		}

	}

}
