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
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.opennms.netmgt.dao.hibernate.DistPollerDaoHibernate;
import org.opennms.netmgt.dao.hibernate.EventDaoHibernate;
import org.opennms.netmgt.dao.hibernate.MonitoringSystemDaoHibernate;
import org.opennms.netmgt.dao.hibernate.NodeDaoHibernate;
import org.opennms.netmgt.dao.hibernate.ServiceTypeDaoHibernate;
import org.opennms.netmgt.eventd.EventIpcManagerDefaultImpl;
import org.opennms.netmgt.eventd.EventUtilDaoImpl;
import org.opennms.netmgt.eventd.Runner;
import org.opennms.netmgt.eventd.UtilMarshler;
import org.opennms.netmgt.eventd.processor.expandable.EventTemplate;
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

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.MessageConsumer;

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

	private static Timer logBroadcastTimer;
	private static Meter eventBroadcastMeter;

	private AtomicBoolean running;

	private ExecutorService backgroundConsumer;
	private EventBus broadCastEventBus;
	public static int eventWriter = 0;
	private static UtilMarshler logMarshler;

	private static final String BROADCAST_EVENTD_CONSUMER_ADDRESS = "broadcast.eventd.message.consumer";

	public EventIpcBroadcastProcessor(MetricRegistry registry) {
		logBroadcastTimer = Objects.requireNonNull(registry).timer("eventlogs.process.broadcast");
		eventBroadcastMeter = registry.meter("events.process.broadcast");
	}

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
		logMarshler = new UtilMarshler(Log.class);
		System.setProperty("opennms.home", "src/test/resources");
		// org.apache.log4j.Logger logger4j = org.apache.log4j.Logger.getRootLogger();
		// logger4j.setLevel(org.apache.log4j.Level.toLevel("ERROR"));
		DeploymentOptions deployment = new DeploymentOptions();
		deployment.setWorker(true);
		deployment.setWorkerPoolSize(Integer.MAX_VALUE);
		deployment.setMultiThreaded(true);
		MetricRegistry registry = new MetricRegistry();
		logBroadcastTimer = Objects.requireNonNull(registry).timer("eventlogs.process.broadcast");
		eventBroadcastMeter = registry.meter("events.process.broadcast");
		m_eventIpcBroadcaster = new EventIpcManagerDefaultImpl(registry);
		Runner.runClusteredExample(EventIpcBroadcastProcessor.class, deployment);
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
			try (Context context = logBroadcastTimer.time()) {
				for (Event eachEvent : eventLog.getEvents().getEvent()) {
					process(eventLog.getHeader(), eachEvent, synchronous);
					eventBroadcastMeter.mark();
				}
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
	public void start(final Future<Void> startedResult) throws Exception {
		running = new AtomicBoolean(true);

		broadCastEventBus = vertx.eventBus();

		backgroundConsumer = Executors.newSingleThreadExecutor();
		backgroundConsumer.submit(() -> {
			try {

				startedResult.complete();
				consume();

			} catch (Exception ex) {
				String error = "Failed to startup";
				startedResult.fail(ex);
			}
		});
	}

	private void consume() {
		while (running.get()) {
			try {
				MessageConsumer<String> broadCastEventConsumer = broadCastEventBus
						.consumer(BROADCAST_EVENTD_CONSUMER_ADDRESS);
				broadCastEventConsumer.handler(message -> {

					try {
						process((Log) logMarshler.unmarshal(message.body()));
						System.out.println("Event at broadcaster " + EventTemplate.eventCount.incrementAndGet());
					} catch (EventProcessorException e) {
						e.printStackTrace();
					}
				});
			} catch (Exception ex) {
			}
		}
	}

}
