/*******************************************************************************
 * This file is part of OpenNMS(R).
 *
 * Copyright (C) 2002-2014 The OpenNMS Group, Inc.
 * OpenNMS(R) is Copyright (C) 1999-2014 The OpenNMS Group, Inc.
 *
 * OpenNMS(R) is a registered trademark of The OpenNMS Group, Inc.
 *
 * OpenNMS(R) is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 *
 * OpenNMS(R) is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with OpenNMS(R).  If not, see:
 *      http://www.gnu.org/licenses/
 *
 * For more information contact:
 *     OpenNMS(R) Licensing <license@opennms.org>
 *     http://www.opennms.org/
 *     http://www.opennms.com/
 *******************************************************************************/

package org.opennms.netmgt.eventd.processor;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.opennms.netmgt.eventd.EventIpcManagerDefaultImpl;
import org.opennms.netmgt.eventd.processor.expandable.EventTemplate;
import org.opennms.netmgt.eventd.util.ClusteredVertx;
import org.opennms.netmgt.eventd.util.ConfigConstants;
import org.opennms.netmgt.eventd.util.UtiliMarshlerUnmarshaler;
import org.opennms.netmgt.events.api.EventIpcBroadcaster;
import org.opennms.netmgt.events.api.EventProcessor;
import org.opennms.netmgt.events.api.EventProcessorException;
import org.opennms.netmgt.xml.event.Event;
import org.opennms.netmgt.xml.event.Header;
import org.opennms.netmgt.xml.event.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.eventbus.EventBus;

/**
 * EventProcessor that broadcasts events to other interested daemons with
 * EventIpcBroadcaster.broadcastNow(Event).
 *
 * @author ranger
 * @version $Id: $
 */
public class EventIpcBroadcastProcessor extends AbstractVerticle implements EventProcessor, InitializingBean {
	private static final Logger LOG = LoggerFactory.getLogger(EventIpcBroadcastProcessor.class);
	private static EventIpcBroadcaster m_eventIpcBroadcaster;

	private ExecutorService backgroundConsumer;
	private EventBus broadCastEventBus;
	private static UtiliMarshlerUnmarshaler logXmlMarshler;

	/**
	 * <p>
	 * afterPropertiesSet
	 * </p>
	 *
	 * @throws java.lang.IllegalStateException
	 *             if any.
	 */
	@Override
	public void afterPropertiesSet() throws IllegalStateException {
		Assert.state(m_eventIpcBroadcaster != null, "property eventIpcBroadcaster must be set");
	}

	public EventIpcBroadcastProcessor() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) throws IOException, Exception {
		logXmlMarshler = new UtiliMarshlerUnmarshaler(Log.class);
		System.setProperty("opennms.home", "src/test/resources");
		m_eventIpcBroadcaster = new EventIpcManagerDefaultImpl();
		DeploymentOptions deployOptions = new DeploymentOptions();
		deployOptions.setWorker(true);
		deployOptions.setWorkerPoolSize(Integer.MAX_VALUE);
		deployOptions.setMultiThreaded(true);
		ClusteredVertx.runClusteredWithDeploymentOptions(EventIpcBroadcastProcessor.class, deployOptions,"Broadcaster");
	}

	/**
	 * If synchronous mode is not specified, the event is broadcasted asynchronously
	 * by default.
	 */
	@Override
	public void process(Log eventLog) throws EventProcessorException {
		process(eventLog, false);
	}

	@Override
	public void process(Log eventLog, boolean synchronous) throws EventProcessorException {
		if (eventLog != null && eventLog.getEvents() != null && eventLog.getEvents().getEvent() != null) {
			for (Event eachEvent : eventLog.getEvents().getEvent()) {
				process(eventLog.getHeader(), eachEvent, synchronous);
			}
		}
	}

	private void process(Header eventHeader, Event event, boolean synchronous) {
		if (event.getLogmsg() != null && event.getLogmsg().getDest().equals("suppress")) {
			LOG.debug("process: skip sending event {} to other daemons because is marked as suppress", event.getUei());
		} else {
			m_eventIpcBroadcaster.broadcastNow(event, synchronous);
		}
	}

	/**
	 * <p>
	 * getEventIpcBroadcaster
	 * </p>
	 *
	 * @return a {@link org.opennms.netmgt.events.api.EventIpcBroadcaster} object.
	 */
	public EventIpcBroadcaster getEventIpcBroadcaster() {
		return m_eventIpcBroadcaster;
	}

	/**
	 * <p>
	 * setEventIpcBroadcaster
	 * </p>
	 *
	 * @param eventIpcManager
	 *            a {@link org.opennms.netmgt.events.api.EventIpcBroadcaster}
	 *            object.
	 */
	public void setEventIpcBroadcaster(EventIpcBroadcaster eventIpcManager) {
		m_eventIpcBroadcaster = eventIpcManager;
	}

	@Override
	public void start() throws Exception {
		broadCastEventBus = vertx.eventBus();

		backgroundConsumer = Executors.newSingleThreadExecutor();
		backgroundConsumer.submit(() -> {
			try {
				consumeFromEventBus();
			} catch (Exception ex) {
				String error = "Failed to startup";
				LOG.error(error + ex.getMessage());
			}
		});
	}

	private synchronized void consumeFromEventBus() {
		try {
			broadCastEventBus.consumer(ConfigConstants.HIBERNATE_TO_EVENT_CONSUMER_ADDRESS, eventLog -> {
				try {
					process((Log) logXmlMarshler.unmarshal((String) eventLog.body()));
					System.out.println("Event at broadcaster " + EventTemplate.eventCount.incrementAndGet());
				} catch (EventProcessorException e) {
					e.printStackTrace();
				}
			});
		} catch (Exception ex) {
		}
	}

}
