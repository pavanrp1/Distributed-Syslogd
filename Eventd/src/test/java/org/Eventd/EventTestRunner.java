package org.Eventd;

import java.io.IOException;

import org.opennms.netmgt.eventd.DefaultEventHandlerImpl;
import org.opennms.netmgt.eventd.EventExpander;
import org.opennms.netmgt.eventd.processor.EventIpcBroadcastProcessor;
import org.opennms.netmgt.eventd.processor.HibernateEventWriter;

public class EventTestRunner {

	public static void main(String[] args) throws IOException, Exception {

		for (int i = 0; i < 1; i++) {
			DefaultEventHandlerImpl.main(args);
			EventExpander.main(args);
			HibernateEventWriter.main(args);
			EventIpcBroadcastProcessor.main(args);
		}

	}

}
