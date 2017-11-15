package org.Eventd;

import java.io.IOException;

import org.opennms.netmgt.eventd.DefaultEventHandlerImpl;
import org.opennms.netmgt.eventd.EventExpander;
import org.opennms.netmgt.eventd.processor.EventIpcBroadcastProcessor;
import org.opennms.netmgt.eventd.processor.HibernateEventWriter;

public class TestRun2 {

	private static DefaultEventHandlerImpl deafultImpl;
	private static EventExpander eventExpander;
	private static HibernateEventWriter hibernateWriter;
	private static EventIpcBroadcastProcessor eventBroadCaster;

	public static void main(String[] args) throws IOException, Exception {

		for (int i = 0; i < 1; i++) {
			deafultImpl = new DefaultEventHandlerImpl();
			eventExpander = new EventExpander();
			hibernateWriter = new HibernateEventWriter();
			eventBroadCaster = new EventIpcBroadcastProcessor();
			deafultImpl.main(args);
			eventExpander.main(args);
			hibernateWriter.main(args);
			eventBroadCaster.main(args);
		}

	}

}
