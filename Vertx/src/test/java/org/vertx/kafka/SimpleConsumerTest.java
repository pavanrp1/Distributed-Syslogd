/**
 * 
 */
package org.vertx.kafka;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

import javax.print.attribute.TextSyntax;

import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opennms.core.ipc.sink.api.MessageConsumer;
import org.opennms.core.ipc.sink.api.SinkModule;
import org.opennms.core.test.ConfigurationTestUtils;
import org.opennms.core.utils.ConfigFileConstants;
import org.opennms.netmgt.config.SyslogdConfigFactory;
import org.opennms.netmgt.dao.api.DistPollerDao;
import org.opennms.netmgt.dao.api.InterfaceToNodeCache;
import org.opennms.netmgt.dao.api.InterfaceToNodeMap;
import org.opennms.netmgt.dao.hibernate.InterfaceToNodeCacheDaoImpl;
import org.opennms.netmgt.dao.mock.MockDistPollerDao;
import org.opennms.netmgt.syslogd.SyslogSinkConsumer;
import org.opennms.netmgt.syslogd.SyslogSinkModule;
import org.opennms.netmgt.syslogd.api.SyslogConnection;
import org.opennms.netmgt.syslogd.api.SyslogMessageLogDTO;
import org.opennms.netmgt.xml.event.Log;
import org.vertx.kafka.util.ConfigConstants;
import org.vertx.kafka.util.MockInterfaceCacheDao;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import io.vertx.ext.unit.junit.VertxUnitRunner;

/**
 * @author pk015603
 *
 */
@RunWith(VertxUnitRunner.class)
public class SimpleConsumerTest {
	private static final Logger logger = LoggerFactory.getLogger(SimpleConsumerTest.class);

	private static Vertx vertx;

	@Rule
	public Timeout timeoutRule = Timeout.seconds(36000000);

	private JsonObject consumerConfig;

	private SyslogdConfigFactory sylogConfig;

	private DistPollerDao m_distPollerDao = null;

	private SyslogSinkConsumer m_sinkConsumer = null;

	private SyslogSinkModule m_sinkModule = null;

	private MessageConsumer<SyslogConnection, SyslogMessageLogDTO> messageConsumer;

	private List<String> grookPatternList;

	private List<String> topics;

	private SimpleConsumer simpleConsumer;

	private VertxOptions vxOptions;

	@Before
	public void setUp() throws Exception {
		SyslogSinkConsumer.eventCount = 0;

		MockInterfaceCacheDao mock = new MockInterfaceCacheDao();
		mock.setNodeId("MalaMac", InetAddress.getByName("localhost"), 1);
		mock.setNodeId("PavanMac", InetAddress.getByName("10.182.241.167"), 2);
		InterfaceToNodeCacheDaoImpl.setInstance(mock);
		loadSyslogConfiguration("/etc/syslogd-loadtest-configuration.xml");
		System.setProperty("opennms.home", "src/test/resources");
		consumerConfig = new JsonObject();
		consumerConfig.put(ConfigConstants.GROUP_ID, "testGroup");
		consumerConfig.put(ConfigConstants.ZK_CONNECT, "localhost:2181");
		consumerConfig.put(ConfigConstants.BOOTSTRAP_SERVERS, "localhost:9092");
		topics = new ArrayList<>();
		topics.add("testGroup");
		consumerConfig.put("topics", new JsonArray(topics));

		grookPatternList = SyslogSinkConsumer.readPropertiesInOrderFrom(
				ConfigFileConstants.getFile(ConfigFileConstants.SYSLOGD_CONFIGURATION_PROPERTIES));
		m_distPollerDao = new MockDistPollerDao();
		m_sinkConsumer = new SyslogSinkConsumer();
		m_sinkConsumer.setGrokPatternsList(grookPatternList);
		m_sinkConsumer.setDistPollerDao(m_distPollerDao);
		m_sinkConsumer.setSyslogdConfig(sylogConfig);
		m_sinkModule = m_sinkConsumer.getModule();
		messageConsumer = new MessageConsumer<SyslogConnection, SyslogMessageLogDTO>() {
			@Override
			public SinkModule<SyslogConnection, SyslogMessageLogDTO> getModule() {
				return m_sinkModule;
			}

			@Override
			public void handleMessage(SyslogMessageLogDTO message) {
			}

		};

		simpleConsumer = new SimpleConsumer(vertx);
		Future<Void> startFuture = Future.future();
		simpleConsumer.setMessageConsumer(messageConsumer);
		simpleConsumer.setVerticleConfig(consumerConfig);
		new DeploymentOptions().setWorker(true);
		vxOptions = new VertxOptions().setBlockedThreadCheckInterval(2000000000);
		vxOptions.setMaxEventLoopExecuteTime(Long.MAX_VALUE);
		simpleConsumer.setSylogConfig(sylogConfig);
	}

	public void testMessageReceipt(TestContext testContext) {
		Async async = testContext.async();
		VertxOptions vxOptions = new VertxOptions().setBlockedThreadCheckInterval(2000000000);
		vxOptions.setMaxEventLoopExecuteTime(Long.MAX_VALUE);
		vertx = Vertx.vertx(vxOptions);
		new DeploymentOptions().setWorker(true);
		vertx.deployVerticle(SimpleConsumer.class.getName(), new DeploymentOptions().setConfig(consumerConfig),
				deploy -> {
					if (deploy.failed()) {
						logger.error("", deploy.cause());
						testContext.fail("Could not deploy verticle");
						async.complete();
						vertx.close();
					} else {
						vertx.eventBus().consumer(SimpleConsumer.EVENTBUS_DEFAULT_ADDRESS,
								(Message<JsonObject> message) -> {
									assertTrue(message.body().toString().length() > 0);
									logger.info("got message: " + message.body());
									SyslogSinkConsumer consume = new SyslogSinkConsumer();
									try {
										consume.setSyslogdConfig(sylogConfig);
										consume.handleMessage(getSyslogMessageLogDTO(message));
									} catch (Exception e) {
										logger.error("Failed to consume syslogd message", e.getMessage());
									}
									System.out.println("Event number recieved " + SyslogSinkConsumer.eventCount);

								});
					}
				});

		async.awaitSuccess();
	}

	protected synchronized SyslogMessageLogDTO getSyslogMessageLogDTO(Message<JsonObject> message) {
		try {
			synchronized (message) {
				return messageConsumer.getModule().unmarshal(message.body().getValue("value").toString());
			}
		} catch (Exception e) {
			logger.error("Unable to load syslogmessagelogdto " + e.getMessage());
			return null;
		}
	}

	private void loadSyslogConfiguration(final String configuration) throws IOException {
		InputStream stream = null;
		try {
			stream = ConfigurationTestUtils.getInputStreamForResource(this, configuration);
			sylogConfig = new SyslogdConfigFactory(stream);
		} finally {
			if (stream != null) {
				IOUtils.closeQuietly(stream);
			}
		}
	}

	@Test
	public void test(TestContext context) {
		try {

			Async async = context.async();
			vertx = Vertx.vertx(vxOptions);
			vertx.deployVerticle(simpleConsumer);
			async.awaitSuccess();

		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}