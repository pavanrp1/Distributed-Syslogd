/**
 * 
 */
package org.vertx.kafka;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opennms.core.ipc.sink.api.MessageConsumer;
import org.opennms.core.ipc.sink.api.SinkModule;
import org.opennms.core.test.ConfigurationTestUtils;
import org.opennms.core.utils.ConfigFileConstants;
import org.opennms.netmgt.config.SyslogdConfigFactory;
import org.opennms.netmgt.dao.api.DistPollerDao;
import org.opennms.netmgt.dao.hibernate.InterfaceToNodeCacheDaoImpl;
import org.opennms.netmgt.dao.mock.MockDistPollerDao;
import org.opennms.netmgt.dao.mock.MockInterfaceToNodeCache;
import org.opennms.netmgt.syslogd.SyslogSinkConsumer;
import org.opennms.netmgt.syslogd.SyslogSinkModule;
import org.opennms.netmgt.syslogd.api.SyslogConnection;
import org.opennms.netmgt.syslogd.api.SyslogMessageLogDTO;
import org.vertx.kafka.util.ConfigConstants;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

/**
 * @author pk015603
 *
 */
@RunWith(VertxUnitRunner.class)
public class SimpleConsumerTest {
	private static final Logger logger = LoggerFactory.getLogger(SimpleConsumerTest.class);

    private static Vertx vertx;
    
    private JsonObject consumerConfig;

	private SyslogdConfigFactory sylogConfig;
	
	private DistPollerDao m_distPollerDao=null;
	
	private SyslogSinkConsumer m_sinkConsumer =null;
	
	private SyslogSinkModule m_sinkModule =null;
	
	private MessageConsumer<SyslogConnection, SyslogMessageLogDTO> messageConsumer;
	
	private List<String> grookPatternList;
	
	private List<String> topics;
	
    @Before
	public void setUp() throws Exception {
    		SyslogSinkConsumer.eventCount=0;
    		InterfaceToNodeCacheDaoImpl.setInstance(new MockInterfaceToNodeCache());
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
		m_distPollerDao=new MockDistPollerDao();
		m_sinkConsumer=new SyslogSinkConsumer();
		m_sinkConsumer.setGrokPatternsList(grookPatternList);
		m_sinkConsumer.setDistPollerDao(m_distPollerDao);
		m_sinkConsumer.setSyslogdConfig(sylogConfig);
		m_sinkModule=m_sinkConsumer.getModule();
		messageConsumer=new MessageConsumer<SyslogConnection, SyslogMessageLogDTO>() {
			@Override
			public SinkModule<SyslogConnection, SyslogMessageLogDTO> getModule() {
				return m_sinkModule;
			}

			@Override
			public void handleMessage(SyslogMessageLogDTO message) {
			}
			
		};
	}

    @Test
    public void testMessageReceipt(TestContext testContext) {
        Async async = testContext.async();

        vertx = Vertx.vertx();

        vertx.deployVerticle(SimpleConsumer.class.getName(),
            new DeploymentOptions().setConfig(consumerConfig), deploy -> {
                if (deploy.failed()) {
                    logger.error("", deploy.cause());
                    testContext.fail("Could not deploy verticle");
                    async.complete();
                    vertx.close();
                } else {
                    // have the test run for 20 seconds to give you enough time to get a message off
//                    long timerId = vertx.setTimer(60000, theTimerId ->
//                    {
//                        logger.info("Failed to get any messages");
//                        testContext.fail("Test did not complete in 20 seconds");
//                        async.complete();
//                        vertx.close();
//                    });

                    logger.info("Registering listener on event bus for kafka messages");

                    vertx.eventBus().consumer(SimpleConsumer.EVENTBUS_DEFAULT_ADDRESS, (Message<JsonObject> message) -> {
                        assertTrue(message.body().toString().length() > 0);
                        logger.info("got message: " + message.body());
                        SyslogSinkConsumer consume = new SyslogSinkConsumer();
                        try {
                        	consume.setSyslogdConfig(sylogConfig);
                        	consume.handleMessage(getSyslogMessageLogDTO(message));
                        } catch (Exception e) {
                        	 logger.error("Failed to consume syslogd message", e.getMessage());
                        }
//                        vertx.cancelTimer(timerId);
//                        async.complete();
//                        vertx.close();
                        System.out.println("Event number recieved "+SyslogSinkConsumer.eventCount);
                    });
                }
            }
        );
    }

	protected SyslogMessageLogDTO getSyslogMessageLogDTO(Message<JsonObject> message) {
		try {
			return messageConsumer.getModule().unmarshal(message.body().getValue("value").toString());
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
}