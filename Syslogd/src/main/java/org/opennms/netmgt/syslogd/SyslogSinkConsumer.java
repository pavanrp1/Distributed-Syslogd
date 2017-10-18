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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.opennms.core.ipc.sink.api.MessageConsumer;
import org.opennms.core.ipc.sink.api.MessageConsumerManager;
import org.opennms.core.logging.Logging;
import org.opennms.core.logging.Logging.MDCCloseable;
import org.opennms.core.test.ConfigurationTestUtils;
import org.opennms.core.utils.ConfigFileConstants;
import org.opennms.core.utils.InetAddressUtils;
import org.opennms.netmgt.config.SyslogdConfig;
import org.opennms.netmgt.config.SyslogdConfigFactory;
import org.opennms.netmgt.dao.api.DistPollerDao;
import org.opennms.netmgt.dao.hibernate.DistPollerDaoHibernate;
import org.opennms.netmgt.dao.mock.MockDistPollerDao;
import org.opennms.netmgt.events.api.EventForwarder;
import org.opennms.netmgt.syslogd.BufferParser.BufferParserFactory;
import org.opennms.netmgt.syslogd.api.SyslogConnection;
import org.opennms.netmgt.syslogd.api.SyslogMessageDTO;
import org.opennms.netmgt.syslogd.api.SyslogMessageLogDTO;
import org.opennms.netmgt.syslogd.api.SyslogdMessageCodec;
import org.opennms.netmgt.xml.event.Events;
import org.opennms.netmgt.xml.event.Log;
import org.opennms.netmgt.xml.event.Parm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.EventBus;

public class SyslogSinkConsumer extends AbstractVerticle
		implements MessageConsumer<SyslogConnection, SyslogMessageLogDTO>, InitializingBean {

	private static final Logger LOG = LoggerFactory.getLogger(SyslogSinkConsumer.class);

	@Autowired
	private MessageConsumerManager messageConsumerManager;

	@Autowired
	private SyslogdConfig syslogdConfig;

	@Autowired
	private DistPollerDao distPollerDao;

	public static int eventCount = 0;

	private static final String SYSLOGD_CONSUMER_ADDRESS = "syslogd.message.consumer";

	private static final String EVENTD_CONSUMER_ADDRESS = "eventd.message.consumer";

	private final Timer consumerTimer;
	private final Timer toEventTimer;
	private Log m_eventLog;

	private EventBus syslogdEventBus;

	private AtomicBoolean concurrentRunning;

	private ExecutorService backgroundConsumer;

	private boolean isMessageCodeRegistered = true;

	public Log getEventLog() {
		return m_eventLog;
	}

	private final static ExecutorService m_executor = Executors.newSingleThreadExecutor();

	private static List<String> grokPatternsList;

	public static List<String> getGrokPatternsList() {
		return grokPatternsList;
	}

	public static String getSyslogdConsumerAddress() {
		return SYSLOGD_CONSUMER_ADDRESS;
	}

	public static String getEventdConsumerAddress() {
		return EVENTD_CONSUMER_ADDRESS;
	}

	public void setGrokPatternsList(List<String> grokPatternsListValue) {
		grokPatternsList = grokPatternsListValue;
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
	static {
		try {
			loadGrokParserList();
		} catch (IOException e) {
			LOG.debug("Failed to load Grok pattern list." + e);
		}

	}

	@Override
	public void start(final Future<Void> startedResult) throws Exception {
		
		System.out.println("Hi");
		syslogdEventBus = vertx.eventBus();

		// syslogSinkConsumer = new SyslogSinkConsumer();
		// SyslogSinkConsumer.eventCount = 0;
		grokPatternsList = SyslogSinkConsumer.readPropertiesInOrderFrom(
				ConfigFileConstants.getFile(ConfigFileConstants.SYSLOGD_CONFIGURATION_PROPERTIES));
		distPollerDao = new DistPollerDaoHibernate();
		syslogdConfig = loadSyslogConfiguration("/etc/syslogd-loadtest-configuration.xml");

		concurrentRunning = new AtomicBoolean(true);

		backgroundConsumer = Executors.newSingleThreadExecutor();
		backgroundConsumer.submit(() -> {

			// to ensure codec register is at start and its only once
			if (isMessageCodeRegistered) {
				syslogdEventBus.registerDefaultCodec(Log.class, new SyslogdMessageCodec());
				isMessageCodeRegistered = false;
			}

			startedResult.complete();
			consumeFromKafkaEventBus();

		});

	}

	/**
	 * Handles looping and consuming
	 */
	private void consumeFromKafkaEventBus() {
		while (concurrentRunning.get()) {
			try {
				io.vertx.core.eventbus.MessageConsumer<SyslogMessageLogDTO> consumerFromEventBus = syslogdEventBus
						.consumer(getSyslogdConsumerAddress());
				consumerFromEventBus.handler(syslogDTOMessage -> {
					handleMessage(syslogDTOMessage.body());
					syslogdEventBus.send(EVENTD_CONSUMER_ADDRESS, getEventLog());
				});

			} catch (Exception e) {
				LOG.error("Failed to consume from Kafka Event Bus : " + e.getMessage());
			}
		}
	}

	public static void loadGrokParserList() throws IOException {
		grokPatternsList = new ArrayList<String>();
		File syslogConfigFile = ConfigFileConstants.getFile(ConfigFileConstants.SYSLOGD_CONFIGURATION_PROPERTIES);
		readPropertiesInOrderFrom(syslogConfigFile);
	}

	@Override
	public synchronized void handleMessage(SyslogMessageLogDTO syslogDTO) {
		try (Context consumerCtx = consumerTimer.time()) {
			try (MDCCloseable mdc = Logging.withPrefixCloseable(Syslogd.LOG4J_CATEGORY)) {
				try (Context toEventCtx = toEventTimer.time()) {
					m_eventLog = toEventLog(syslogDTO);

				}
			}
		}
	}

	public Log toEventLog(SyslogMessageLogDTO messageLog) {
		final Log elog = new Log();
		final Events events = new Events();
		elog.setEvents(events);
		for (SyslogMessageDTO message : messageLog.getMessages()) {
			try {
				LOG.debug("Converting syslog message into event.");
				ConvertToEvent re = new ConvertToEvent(messageLog.getSystemId(), messageLog.getLocation(),
						messageLog.getSourceAddress(), messageLog.getSourcePort(),
						// Decode the packet content as ASCII
						// TODO: Support more character encodings?
						StandardCharsets.US_ASCII.decode(message.getBytes()).toString(), syslogdConfig,
						parse(message.getBytes()));
				events.addEvent(re.getEvent());
				eventCount++;
				System.out.println("Events at consumer " + eventCount);
			} catch (final UnsupportedEncodingException e) {
				LOG.info("Failure to convert package", e);
			} catch (final MessageDiscardedException e) {
				LOG.info("Message discarded, returning without enqueueing event.", e);
			} catch (final Throwable e) {
				LOG.error("Unexpected exception while processing SyslogConnection", e);
			}
		}
		return elog;
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

	public void setSyslogdConfig(SyslogdConfig syslogdConfig) {
		this.syslogdConfig = syslogdConfig;
	}

	public void setDistPollerDao(DistPollerDao distPollerDao) {
		this.distPollerDao = distPollerDao;
	}

	/**
	 * This method will parse the message against the grok patterns
	 * 
	 * @param messageBytes
	 * 
	 * @return Parameter list
	 */
	public static Map<String, String> parse(ByteBuffer messageBytes) {
		String grokPattern;
		Map<String, String> paramsMap = new HashMap<String, String>();
		if (null == getGrokPatternsList() || getGrokPatternsList().isEmpty()) {
			LOG.error("No Grok Pattern has been defined");
			return null;
		}
		for (int i = 0; i < getGrokPatternsList().size(); i++) {
			grokPattern = getGrokPatternsList().get(i);
			BufferParserFactory grokFactory = GrokParserFactory.parseGrok(grokPattern);
			ByteBuffer incoming = ByteBuffer.wrap(messageBytes.array());
			try {
				paramsMap = loadParamsMap(
						grokFactory.parse(incoming.asReadOnlyBuffer(), m_executor).get().getParmCollection());
				return paramsMap;
			} catch (InterruptedException | ExecutionException e) {
				// LOG.debug("Parse Exception occured !!!Grok Pattern "+grokPattern+" didn't
				// match");
				continue;
			}
		}
		return null;

	}

	public static Map<String, String> loadParamsMap(List<Parm> paramsList) {
		return paramsList.stream().collect(Collectors.toMap(Parm::getParmName, param -> param.getValue().getContent(),
				(paramKey1, paramKey2) -> paramKey2));
	}

	public static List<String> readPropertiesInOrderFrom(File syslogdConfigdFile) throws IOException {
		InputStream propertiesFileInputStream = new FileInputStream(syslogdConfigdFile);
		Set<String> grookSet = new LinkedHashSet<String>();
		final Properties properties = new Properties();
		final BufferedReader reader = new BufferedReader(new InputStreamReader(propertiesFileInputStream));

		String bufferedReader = reader.readLine();

		while (bufferedReader != null) {
			final ByteArrayInputStream lineStream = new ByteArrayInputStream(bufferedReader.getBytes("ISO-8859-1"));
			properties.load(lineStream);

			final Enumeration<?> propertyNames = properties.<String>propertyNames();

			if (propertyNames.hasMoreElements()) {

				final String paramKey = (String) propertyNames.nextElement();
				final String paramsValue = properties.getProperty(paramKey);

				grookSet.add(paramsValue);
				properties.clear();
			}
			bufferedReader = reader.readLine();
		}
		grokPatternsList = new ArrayList<String>(grookSet);
		reader.close();
		return grokPatternsList;
	}

	private SyslogdConfigFactory loadSyslogConfiguration(final String configuration) throws IOException {
		InputStream stream = null;
		try {
			stream = ConfigurationTestUtils.getInputStreamForResource(this, configuration);
			return new SyslogdConfigFactory(stream);
		} finally {
			if (stream != null) {
				IOUtils.closeQuietly(stream);
			}
		}
	}

}
