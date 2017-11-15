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

package org.opennms.netmgt.eventd;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.opennms.netmgt.dao.api.NodeDao;
import org.opennms.netmgt.eventd.util.ClusteredVertx;
import org.opennms.netmgt.eventd.util.ConfigConstants;
import org.opennms.netmgt.eventd.util.UtiliMarshlerUnmarshaler;
import org.opennms.netmgt.events.api.EventHandler;
import org.opennms.netmgt.events.api.EventProcessor;
import org.opennms.netmgt.model.OnmsNode;
import org.opennms.netmgt.xml.event.Event;
import org.opennms.netmgt.xml.event.Events;
import org.opennms.netmgt.xml.event.Log;
import org.opennms.netmgt.xml.event.Parm;
import org.springframework.util.Assert;

import com.codahale.metrics.MetricRegistry;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

/**
 * The EventHandler builds Runnables that essentially do all the work on an
 * incoming event.
 *
 * Operations done on an incoming event are handled by the List of injected
 * EventProcessors, in the order in which they are given in the list. If any of
 * them throw an exception, further processing of that event Log is stopped.
 *
 * @author <A HREF="mailto:sowmya@opennms.org">Sowmya Nataraj </A>
 * @author <A HREF="http://www.opennms.org">OpenNMS.org </A>
 */
public class DefaultEventHandlerImpl extends AbstractVerticle implements EventHandler {

	private static final Logger LOG = LoggerFactory.getLogger(DefaultEventHandlerImpl.class);

	private List<EventProcessor> m_eventProcessors;

	private boolean m_logEventSummaries;

	private static Log m_eventdLog;

	public static Log getEventdLog() {
		return m_eventdLog;
	}

	public static void setEventdLog(Log m_eventdLog) {
		DefaultEventHandlerImpl.m_eventdLog = m_eventdLog;
	}

	private boolean isWaitingForLoginModule = true;

	private NodeDao m_nodeDao;

	private ExecutorService backgroundConsumer;

	private EventBus eventIpcEventBus;

	private static UtiliMarshlerUnmarshaler eventXmlHandler;

	private static UtiliMarshlerUnmarshaler logXmlHandler;

	public DefaultEventHandlerImpl() {
	}

	/**
	 * <p>
	 * Constructor for DefaultEventHandlerImpl.
	 * </p>
	 */
	public DefaultEventHandlerImpl(MetricRegistry registry) {
		Objects.requireNonNull(registry).timer("eventlogs.process");
		registry.histogram("eventlogs.sizes");
	}

	public EventHandlerRunnable createRunnable(Event eventLog) {
		return new EventHandlerRunnable(eventLog, false);
	}

	public EventHandlerRunnable createRunnable(Event eventLog, boolean synchronous) {
		return new EventHandlerRunnable(eventLog, synchronous);
	}

	private class EventHandlerRunnable implements Runnable {
		/**
		 * log of events
		 */
		private final Event m_eventLog;

		public EventHandlerRunnable(Event eventLog, boolean synchronous) {
			Assert.notNull(eventLog, "eventLog argument must not be null");

			m_eventLog = eventLog;
		}

		/**
		 * Process the received events. For each event, use the EventExpander to look up
		 * matching eventconf entry and load info from that match, expand event parms,
		 * add event to database and send event to appropriate listeners.
		 */
		@Override
		public void run() {

			// This will wait for 25 seconds to get login module up and also
			// for karaf and its features to get loaded
			// if (isWaitingForLoginModule) {
			// try {
			// Thread.sleep(25000);
			// } catch (InterruptedException e) {
			// isWaitingForLoginModule = false;
			// }
			// isWaitingForLoginModule = false;
			// }

			// no events to process
			Event event = m_eventLog;
			if (event.getNodeid() == 0)

			{

				final Parm foreignSource = event.getParm("_foreignSource");
				if (foreignSource != null && foreignSource.getValue() != null) {
					final Parm foreignId = event.getParm("_foreignId");
					if (foreignId != null && foreignId.getValue() != null) {
						final OnmsNode node = getNodeDao().findByForeignId(foreignSource.getValue().getContent(),
								foreignId.getValue().getContent());
						if (node != null) {
							event.setNodeid(node.getId().longValue());
						} else {
							LOG.warn("Can't find node associated with foreignSource {} and foreignId {}", foreignSource,
									foreignId);
						}
					}
				}
			}

			if (LOG.isInfoEnabled() &&

					getLogEventSummaries()) {
				LOG.info("Received event: UEI={}, src={}, iface={}, svc={}, time={}, parms={}", event.getUei(),
						event.getSource(), event.getInterface(), event.getService(), event.getTime(),
						getPrettyParms(event));
			}

			if (LOG.isDebugEnabled()) {
				// Log the uei, source, and other important aspects
				final String uuid = event.getUuid();
				LOG.debug("Event {");
				LOG.debug("  uuid  = {}", (uuid != null && uuid.length() > 0 ? uuid : "<not-set>"));
				LOG.debug("  uei   = {}", event.getUei());
				LOG.debug("  src   = {}", event.getSource());
				LOG.debug("  iface = {}", event.getInterface());
				LOG.debug("  svc   = {}", event.getService());
				LOG.debug("  time  = {}", event.getTime());
				// NMS-8413: I'm seeing a ConcurrentModificationException in the logs here,
				// copy the parm collection to avoid this
				List<Parm> parms = new ArrayList<>(event.getParmCollection());
				if (parms.size() > 0) {
					LOG.debug("  parms {");
					for (final Parm parm : parms) {
						if ((parm.getParmName() != null) && (parm.getValue().getContent() != null)) {
							LOG.debug("    ({}, {})", parm.getParmName().trim(), parm.getValue().getContent().trim());
						}
					}
					LOG.debug("  }");
				}
				LOG.debug("}");
			}
			Events events = new Events();
			events.addEvent(event);

			Log eventLog = new Log();
			eventLog.setEvents(events);
			eventIpcEventBus.send(ConfigConstants.EVENTEXPANDER_TO_EVENT_CONSUMER_ADDRESS,
					logXmlHandler.marshal(eventLog));

		}

	}

	private static List<String> getPrettyParms(final Event event) {
		final List<String> parms = new ArrayList<>();
		for (final Parm p : event.getParmCollection()) {
			parms.add(p.getParmName() + "=" + p.getValue().getContent());
		}
		return parms;
	}

	public static void main(String[] args) {
		eventXmlHandler = new UtiliMarshlerUnmarshaler(Event.class);
		logXmlHandler = new UtiliMarshlerUnmarshaler(Log.class);
		System.setProperty(ConfigConstants.OPENNMS_HOME, "src/test/resources");
		DeploymentOptions deployOptions = new DeploymentOptions();
		deployOptions.setWorker(true);
		deployOptions.setWorkerPoolSize(Integer.MAX_VALUE);
		deployOptions.setMultiThreaded(true);
		ClusteredVertx.runClusteredWithDeploymentOptions(DefaultEventHandlerImpl.class, deployOptions);

	}

	@Override
	public void start() throws Exception {
		eventIpcEventBus = vertx.eventBus();

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
			eventIpcEventBus.consumer(ConfigConstants.DEFAULT_TO_EVENT_CONSUMER_ADDRESS, eventLog -> {
				sendNowSyncEvent((Event) eventXmlHandler.unmarshal((String) eventLog.body()));
			});
		} catch (Exception ex) {
			LOG.error("Failed to start up ! " + ex.getMessage());
		}
	}

	public void sendNowSyncEvent(Event event) {
		Objects.requireNonNull(event);
		createRunnable(event, true).run();
	}

	public void sendNowSyncLog(Log event) {
		createRunnable(event, true).run();
	}

	public Log getEventLog(Event event) {
		Events events = new Events();
		events.addEvent(event);

		Log eventLog = new Log();
		eventLog.setEvents(events);
		return eventLog;
	}

	/**
	 * <p>
	 * getEventProcessors
	 * </p>
	 *
	 * @return a {@link java.util.List} object.
	 */
	public List<EventProcessor> getEventProcessors() {
		return m_eventProcessors;
	}

	/**
	 * <p>
	 * setEventProcessors
	 * </p>
	 *
	 * @param eventProcessors
	 *            a {@link java.util.List} object.
	 */
	public void setEventProcessors(List<EventProcessor> eventProcessors) {
		m_eventProcessors = eventProcessors;
	}

	public boolean getLogEventSummaries() {
		return m_logEventSummaries;
	}

	public void setLogEventSummaries(final boolean logEventSummaries) {
		m_logEventSummaries = logEventSummaries;
	}

	public void setNodeDao(NodeDao nodeDao) {
		m_nodeDao = nodeDao;
	}

	public NodeDao getNodeDao() {
		return m_nodeDao;
	}

	@Override
	public Runnable createRunnable(Log eventLog, boolean synchronous) {
		return null;
	}

	@Override
	public Runnable createRunnable(Log eventLog) {
		return null;
	}
}
