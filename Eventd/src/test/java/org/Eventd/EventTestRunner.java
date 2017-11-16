package org.Eventd;

import java.io.IOException;

import org.opennms.netmgt.eventd.DefaultEventHandlerImpl;
import org.opennms.netmgt.eventd.EventExpander;
import org.opennms.netmgt.eventd.processor.EventIpcBroadcastProcessor;
import org.opennms.netmgt.eventd.processor.HibernateEventWriter;

public class EventTestRunner {

	private static DefaultEventHandlerImpl defaultImpl;
	private static EventExpander eventExpander;
	private static HibernateEventWriter hibernateWriter;
	private static EventIpcBroadcastProcessor broadcaster;

	public static void main(String[] args) throws IOException, Exception {

		// for (int i = 0; i < 1; i++) {
		defaultImpl = new DefaultEventHandlerImpl();
		eventExpander = new EventExpander();
		hibernateWriter = new HibernateEventWriter();
		broadcaster = new EventIpcBroadcastProcessor();
		defaultImpl.main(args);
		eventExpander.main(args);
		hibernateWriter.main(args);
		broadcaster.main(args);
		// }

	}

}
