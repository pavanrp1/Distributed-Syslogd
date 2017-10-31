/*******************************************************************************
 * This file is part of OpenNMS(R).
 *
 * Copyright (C) 2002-2016 The OpenNMS Group, Inc.
 * OpenNMS(R) is Copyright (C) 1999-2016 The OpenNMS Group, Inc.
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

package org.opennms.netmgt.syslogd;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.io.IOUtils;
import org.opennms.core.ipc.sink.api.MessageConsumer;
import org.opennms.core.ipc.sink.api.MessageConsumerManager;
import org.opennms.core.utils.InetAddressUtils;
import org.opennms.netmgt.config.SyslogdConfig;
import org.opennms.netmgt.config.SyslogdConfigFactory;
import org.opennms.netmgt.dao.api.DistPollerDao;
import org.opennms.netmgt.dao.mock.MockDistPollerDao;
import org.opennms.netmgt.events.api.EventForwarder;
import org.opennms.netmgt.syslogd.api.Runner;
import org.opennms.netmgt.syslogd.api.SyslogConnection;
import org.opennms.netmgt.syslogd.api.SyslogMessageLogDTO;
import org.opennms.netmgt.syslogd.api.SyslogdMessageCodec;
import org.opennms.netmgt.xml.event.Log;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class SyslogSinkConsumer extends AbstractVerticle
		implements MessageConsumer<SyslogConnection, SyslogMessageLogDTO>, InitializingBean {

	private static final Logger LOG = LoggerFactory.getLogger(SyslogSinkConsumer.class);

	@Autowired
	private MessageConsumerManager messageConsumerManager;

	@Autowired
	private static SyslogdConfig syslogdConfig;

	@Autowired
	private static DistPollerDao distPollerDao;

	public static int eventCount = 0;

	private static final String SYSLOGD_CONSUMER_ADDRESS = "syslogd.message.consumer";

	private static final String EVENTD_CONSUMER_ADDRESS = "eventd.message.consumer";

	private final Timer consumerTimer;
	private final Timer toEventTimer;

	private ExecutorService backgroundConsumer;

	private Log m_eventLog;

	private EventBus syslogdEventBus;

	public Log getEventLog() {
		return m_eventLog;
	}

	private final static ExecutorService m_executor = Executors.newSingleThreadExecutor();

	public static String getSyslogdConsumerAddress() {
		return SYSLOGD_CONSUMER_ADDRESS;
	}

	public static String getEventdConsumerAddress() {
		return EVENTD_CONSUMER_ADDRESS;
	}

	public static void main(String[] args) throws IOException {
		distPollerDao = new MockDistPollerDao();
		syslogdConfig = loadSyslogConfiguration("/syslogd-loadtest-configuration.xml");
		Runner.runClusteredExample(SyslogSinkConsumer.class);
	}

	public SyslogSinkConsumer() {
		consumerTimer = new Timer();
		toEventTimer = new Timer();
		new Timer();
		InetAddressUtils.getLocalHostName();
	}

	public SyslogSinkConsumer(MetricRegistry registry) {
		consumerTimer = registry.timer("consumer");
		toEventTimer = registry.timer("consumer.toevent");
		registry.timer("consumer.broadcast");
		InetAddressUtils.getLocalHostName();
	}

	@Override
	public SyslogSinkModule getModule() {
		return new SyslogSinkModule(syslogdConfig, distPollerDao);
	}

	/**
	 * Static block to load grokPatterns during the start of SyslogSink class call.
	 */
	// static {
	// try {
	// grokPatternsList = SyslogSinkConsumer.readPropertiesInOrderFrom(
	// ConfigFileConstants.getFile(ConfigFileConstants.SYSLOGD_CONFIGURATION_PROPERTIES));
	// distPollerDao = new DistPollerDaoHibernate();
	// syslogdConfig =
	// loadSyslogConfiguration("/etc/syslogd-loadtest-configuration.xml");
	// } catch (IOException e) {
	// LOG.debug("Failed to load Grok pattern list." + e);
	// }
	//
	// }

	@Override
	public void start() throws Exception {
		syslogdEventBus = vertx.eventBus();
//		backgroundConsumer = Executors.newSingleThreadExecutor();
//		backgroundConsumer.submit(() -> {
			consumeFromKafkaEventBus();
//		});

	}

	/**
	 * Handles looping and consuming
	 */
	private void consumeFromKafkaEventBus() {
		try {
			io.vertx.core.eventbus.MessageConsumer<String> consumerFromEventBus = syslogdEventBus
					.consumer(getSyslogdConsumerAddress());
			consumerFromEventBus.handler(syslogDTOMessage -> {
				handleMessage(getSyslogMessageLogDTO(syslogDTOMessage.body()));
			});

		} catch (Exception e) {
			LOG.error("Failed to consume from Kafka Event Bus : " + this + e.getMessage());
		}
	}

	@Override
	public void handleMessage(SyslogMessageLogDTO syslogDTO) {
		syslogDTO.setSyslogdConfig(syslogdConfig);
		System.out.println(syslogDTO);
		syslogdEventBus.send(EVENTD_CONSUMER_ADDRESS, syslogDTO);
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		// Automatically register the consumer on initialization
		messageConsumerManager.registerConsumer(this);
	}

	public void setEventForwarder(EventForwarder eventForwarder) {
	}

	public void setMessageConsumerManager(MessageConsumerManager messageConsumerManager) {
		this.messageConsumerManager = messageConsumerManager;
	}

	public synchronized SyslogMessageLogDTO getSyslogMessageLogDTO(String message) {
		try {
			return getModule().unmarshal(message);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	private static SyslogdConfigFactory loadSyslogConfiguration(final String configuration) throws IOException {
		InputStream stream = null;
		try {
			stream = new SyslogSinkConsumer().getClass().getResourceAsStream(configuration);
			return new SyslogdConfigFactory(stream);
		} finally {
			if (stream != null) {
				IOUtils.closeQuietly(stream);
			}
		}
	}

}
