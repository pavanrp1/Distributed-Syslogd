package org.opennms.netmgt.syslogd;

/*******************************************************************************
 * This file is part of OpenNMS(R).
 *
 * Copyright (C) 2016-2016 The OpenNMS Group, Inc.
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

import static org.junit.Assert.fail;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Test;
import org.opennms.core.test.ConfigurationTestUtils;
import org.opennms.netmgt.config.SyslogdConfig;
import org.opennms.netmgt.config.SyslogdConfigFactory;
import org.opennms.netmgt.dao.api.DistPollerDao;
import org.opennms.netmgt.dao.api.MonitoringLocationDao;
import org.opennms.netmgt.dao.hibernate.InterfaceToNodeCacheDaoImpl;
import org.opennms.netmgt.dao.mock.MockInterfaceToNodeCache;
import org.opennms.netmgt.syslogd.BufferParser.BufferParserFactory;
import org.opennms.netmgt.syslogd.ConvertToEvent;
import org.opennms.netmgt.syslogd.GrokParserFactory;
import org.opennms.netmgt.syslogd.MessageDiscardedException;
import org.opennms.netmgt.syslogd.SyslogSinkConsumer;
import org.opennms.netmgt.xml.event.Event;
import org.opennms.netmgt.xml.event.Parm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Convert to event junit test file to test the performance of Syslogd
 * ConvertToEvent processor
 * 
 * @author ms043660
 */
public class ConvertToEventTest {

	private static final Logger LOG = LoggerFactory.getLogger(ConvertToEventTest.class);

	private final ExecutorService m_executor = Executors.newSingleThreadExecutor();

	/**
	 * Test method which calls the ConvertToEvent constructor.
	 * 
	 * @throws MarshalException
	 * @throws ValidationException
	 * @throws IOException
	 */
	@Test
	public void testConvertToEvent() throws IOException {

		InterfaceToNodeCacheDaoImpl.setInstance(new MockInterfaceToNodeCache());

		// 10000 sample syslogmessages from xml file are taken and passed as
		InputStream stream = ConfigurationTestUtils.getInputStreamForResource(this,
				"/etc/syslogd-loadtest-configuration.xml");
		// Inputstream to create syslogdconfiguration
		// InputStream stream = new FileInputStream(
		// "/Users/ms043660/OneDrive - Cerner
		// Corporation/Office/ProjectWorkspace/POC/Vertex/syslog/src/test/resources/etc/syslogd-loadtest-configuration.xml");
		SyslogdConfig config = new SyslogdConfigFactory(stream);

		// Sample message which is embedded in packet and passed as parameter
		// to
		// ConvertToEvent constructor
		byte[] bytes = "<34> 2010-08-19 localhost foo10000: load test 10000 on tty1".getBytes();

		// Datagram packet which is passed as parameter for ConvertToEvent
		// constructor
		DatagramPacket pkt = new DatagramPacket(bytes, bytes.length, InetAddress.getLocalHost(), 5140);
		String data = StandardCharsets.US_ASCII.decode(ByteBuffer.wrap(pkt.getData())).toString();

		// ConvertToEvent takes 4 parameter
		// @param addr The remote agent's address.
		// @param port The remote agent's port
		// @param data The XML data in US-ASCII encoding.
		// @param len The length of the XML data in the buffer
		try {
			String pattern = "<%{INTEGER:facilityCode}> %{INTEGER:year}-%{INTEGER:month}-%{INTEGER:day} %{STRING:hostname} %{STRING:processName}: %{STRING:message}";
			ByteBuffer message = ByteBuffer.wrap(bytes);
			ConvertToEvent convertToEvent = new ConvertToEvent(DistPollerDao.DEFAULT_DIST_POLLER_ID,
					MonitoringLocationDao.DEFAULT_MONITORING_LOCATION_ID, pkt.getAddress(), pkt.getPort(), data, config,
					SyslogSinkConsumer.loadParamsMap(getParamsList(message, pattern)));
			LOG.info("Generated event: {}", convertToEvent.getEvent().toString());
		} catch (MessageDiscardedException | ParseException | InterruptedException | ExecutionException e) {
			LOG.error("Message Parsing failed", e);
			fail("Message Parsing failed: " + e.getMessage());
		}
	}

	/**
	 * Method to generate Params List matching with grok pattern
	 **/
	private List<Parm> getParamsList(ByteBuffer message, String pattern)
			throws InterruptedException, ExecutionException {
		BufferParserFactory grokFactory = GrokParserFactory.parseGrok(pattern);

		CompletableFuture<Event> event = null;

		event = grokFactory.parse(message.asReadOnlyBuffer(), m_executor);
		event.whenComplete((e, ex) -> {
			if (ex == null) {
				// System.out.println(e.toString());
			} else {
				ex.printStackTrace();
			}
		});

		return event.get().getParmCollection();
	}
}