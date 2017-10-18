package org.vertx.kafka;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opennms.core.ipc.sink.api.MessageConsumer;
import org.opennms.core.ipc.sink.api.SinkModule;
import org.opennms.core.test.ConfigurationTestUtils;
import org.opennms.core.utils.ConfigFileConstants;
import org.opennms.netmgt.config.DefaultEventConfDao;
import org.opennms.netmgt.config.SyslogdConfigFactory;
import org.opennms.netmgt.dao.api.DistPollerDao;
import org.opennms.netmgt.dao.api.EventDao;
import org.opennms.netmgt.dao.api.MonitoringSystemDao;
import org.opennms.netmgt.dao.api.ServiceTypeDao;
import org.opennms.netmgt.dao.hibernate.DistPollerDaoHibernate;
import org.opennms.netmgt.dao.hibernate.EventDaoHibernate;
import org.opennms.netmgt.dao.hibernate.InterfaceToNodeCacheDaoImpl;
import org.opennms.netmgt.dao.hibernate.MonitoredServiceDaoHibernate;
import org.opennms.netmgt.dao.hibernate.MonitoringSystemDaoHibernate;
import org.opennms.netmgt.dao.hibernate.NodeDaoHibernate;
import org.opennms.netmgt.dao.hibernate.ServiceTypeDaoHibernate;
import org.opennms.netmgt.dao.mock.MockDistPollerDao;
import org.opennms.netmgt.dao.mock.MockEventDao;
import org.opennms.netmgt.dao.mock.MockNodeDao;
import org.opennms.netmgt.dao.mock.MockServiceTypeDao;
import org.opennms.netmgt.eventd.DefaultEventHandlerImpl;
import org.opennms.netmgt.eventd.EventExpander;
import org.opennms.netmgt.eventd.EventIpcManagerDefaultImpl;
import org.opennms.netmgt.eventd.EventUtilDaoImpl;
import org.opennms.netmgt.eventd.processor.EventIpcBroadcastProcessor;
import org.opennms.netmgt.eventd.processor.HibernateEventWriter;
import org.opennms.netmgt.eventd.processor.HibernateSessionFactory;
import org.opennms.netmgt.syslogd.SyslogSinkConsumer;
import org.opennms.netmgt.syslogd.SyslogSinkModule;
import org.opennms.netmgt.syslogd.api.SyslogConnection;
import org.opennms.netmgt.syslogd.api.SyslogMessageLogDTO;
import org.springframework.core.io.FileSystemResource;
import org.vertx.kafka.util.ConfigConstants;
import org.vertx.kafka.util.MockInterfaceCacheDao;

import com.codahale.metrics.MetricRegistry;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import io.vertx.ext.unit.junit.VertxUnitRunner;

/**
 * @author ms043660
 *
 */
@RunWith(VertxUnitRunner.class)
public class KafkaMessageConsumerTest {

	private static final Logger logger = LoggerFactory.getLogger(KafkaMessageConsumerTest.class);

	@Rule
	public Timeout timeoutRule = Timeout.seconds(Long.MAX_VALUE);

	private KafkaMessageConsumer kafkaMessageConsumer;

	private SyslogSinkConsumer syslogSinkConsumer;

	private static Vertx vertx;

	private JsonObject consumerConfig;

	private List<String> topics;

	private VertxOptions vxOptions;

	private SyslogSinkModule sinkModule = null;

	private MessageConsumer<SyslogConnection, SyslogMessageLogDTO> messageConsumer;

	private EventExpander eventExpander;

	private EventIpcManagerDefaultImpl eventImpl;

	private MetricRegistry metric = new MetricRegistry();

	private EventIpcBroadcastProcessor eventBroadCaster;

	private HibernateEventWriter hibernateWriter;

	private HibernateSessionFactory hibernateSessionFactory;

	@Before
	public void setUp() throws Exception {

		System.setProperty("opennms.home", "src/test/resources");

		KafkaProperties();

		syslogSinkConsumer = SyslogSinkProperties();

		kafkaMessageConsumer = KafkaMessageConsumerProperties();

		EventImplProperties();

		VertxOptionProperties();

	}

	private void EventImplProperties() throws Exception {
		eventImpl = new EventIpcManagerDefaultImpl(metric);

		DefaultEventHandlerImpl defaultEventHandler = new DefaultEventHandlerImpl(metric);
		eventImpl.setEventHandler(defaultEventHandler);

		eventExpander = new EventExpander(metric);
		DefaultEventConfDao eventConfDao = new DefaultEventConfDao();

		hibernateSessionFactory = new HibernateSessionFactory();

		// TODO
		File test = new File(
				"/Users/ms043660/OneDrive - Cerner Corporation/Office/ProjectWorkspace/DistributedSyslogdPoc/Distributed-Syslogd/Vertx/src/test/resources/etc/eventconf.xml");
		eventConfDao.setConfigResource(new FileSystemResource(test));
		eventConfDao.afterPropertiesSet();
		eventExpander.setEventConfDao(eventConfDao);

		EventUtilDaoImpl eventutil = new EventUtilDaoImpl(metric);
		eventExpander.setEventUtil(eventutil);
		eventExpander.afterPropertiesSet();

		eventBroadCaster = new EventIpcBroadcastProcessor(metric);
		eventBroadCaster.setEventIpcBroadcaster(eventImpl);

		hibernateWriter = new HibernateEventWriter(metric);
		hibernateWriter.setEventUtil(eventutil);
		hibernateWriter.setDistPollerDao(new DistPollerDaoHibernate());
		hibernateWriter.setServiceTypeDao(new ServiceTypeDaoHibernate());
		hibernateWriter.setEventDao(new EventDaoHibernate());
		hibernateWriter.setNodeDao(new NodeDaoHibernate());
		hibernateWriter.setMonitoringSystemDao(new MonitoringSystemDaoHibernate());

		hibernateWriter.setTransactionManager(hibernateSessionFactory.getTransactionTemplate());
	}

	private SyslogSinkConsumer SyslogSinkProperties() throws Exception {
		SyslogSinkConsumer syslogSinkConsumer;
		MockInterfaceCacheDao mock = new MockInterfaceCacheDao();
		mock.setNodeId("MalaMac", InetAddress.getByName("localhost"), 1);
		mock.setNodeId("PavanMac", InetAddress.getByName("10.182.241.167"), 2);
		InterfaceToNodeCacheDaoImpl.setInstance(mock);

		syslogSinkConsumer = new SyslogSinkConsumer();
		SyslogSinkConsumer.eventCount = 0;
		syslogSinkConsumer.setGrokPatternsList(SyslogSinkConsumer.readPropertiesInOrderFrom(
				ConfigFileConstants.getFile(ConfigFileConstants.SYSLOGD_CONFIGURATION_PROPERTIES)));
		syslogSinkConsumer.setDistPollerDao(new MockDistPollerDao());
		syslogSinkConsumer.setSyslogdConfig(loadSyslogConfiguration("/etc/syslogd-loadtest-configuration.xml"));
		sinkModule = syslogSinkConsumer.getModule();
		return syslogSinkConsumer;

	}

	private KafkaMessageConsumer KafkaMessageConsumerProperties() {
		KafkaMessageConsumer kafkaMessageConsumer;
		messageConsumer = new MessageConsumer<SyslogConnection, SyslogMessageLogDTO>() {
			@Override
			public SinkModule<SyslogConnection, SyslogMessageLogDTO> getModule() {
				return sinkModule;
			}

			@Override
			public void handleMessage(SyslogMessageLogDTO message) {
			}

		};
		kafkaMessageConsumer = new KafkaMessageConsumer(vertx);
		kafkaMessageConsumer.setVerticleConfig(consumerConfig);
		kafkaMessageConsumer.setMessageConsumer(messageConsumer);
		return kafkaMessageConsumer;

	}

	private void VertxOptionProperties() {
		vxOptions = new VertxOptions().setBlockedThreadCheckInterval(2000000000);
		vxOptions.setMaxEventLoopExecuteTime(Long.MAX_VALUE);
	}

	private void KafkaProperties() {

		consumerConfig = new JsonObject();
		consumerConfig.put(ConfigConstants.GROUP_ID, "syslogd");
		consumerConfig.put(ConfigConstants.ZK_CONNECT, "localhost:2181");
		consumerConfig.put(ConfigConstants.BOOTSTRAP_SERVERS, "localhost:9092");
		topics = new ArrayList<String>();
		topics.add("syslogd");
		consumerConfig.put("topics", new JsonArray(topics));

	}

	@Test
	public void testForKafkaMessageConsumer(TestContext context) {
		try {

			Async asyncRunnable = context.async();
			vertx = Vertx.vertx(vxOptions);
			vertx.deployVerticle(kafkaMessageConsumer);
			vertx.deployVerticle(syslogSinkConsumer);
			vertx.deployVerticle(eventImpl);
			vertx.deployVerticle(eventExpander);
			vertx.deployVerticle(hibernateWriter);
			vertx.deployVerticle(eventBroadCaster);
			asyncRunnable.awaitSuccess();
		} catch (Exception e) {
			e.printStackTrace();
		}

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
