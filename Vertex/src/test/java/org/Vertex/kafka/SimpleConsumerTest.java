/**
 * 
 */
package org.Vertex.kafka;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.Vertx.kafka.SimpleConsumer;
import org.Vertx.kafka.util.ConfigConstants;
import org.Vertx.kafka.util.KafkaEvent;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opennms.core.test.ConfigurationTestUtils;
import org.opennms.netmgt.config.SyslogdConfig;
import org.opennms.netmgt.config.SyslogdConfigFactory;
import org.opennms.netmgt.syslogd.SyslogSinkConsumer;
import org.opennms.netmgt.syslogd.api.SyslogConnection;
import org.opennms.netmgt.syslogd.api.SyslogMessageDTO;
import org.opennms.netmgt.syslogd.api.SyslogMessageLogDTO;

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

//    @Ignore("This is an integration test comment out to actually run it")
    @Test
    public void testMessageReceipt(TestContext testContext) {
        Async async = testContext.async();

        vertx = Vertx.vertx();

        JsonObject consumerConfig = new JsonObject();
        consumerConfig.put(ConfigConstants.GROUP_ID, "testGroup");
        consumerConfig.put(ConfigConstants.ZK_CONNECT, "localhost:2181");
        consumerConfig.put(ConfigConstants.BOOTSTRAP_SERVERS, "localhost:9092");
        List<String> topics = new ArrayList<>();
        topics.add("testGroup");
        consumerConfig.put("topics", new JsonArray(topics));

        vertx.deployVerticle(SimpleConsumer.class.getName(),
            new DeploymentOptions().setConfig(consumerConfig), deploy -> {
                if (deploy.failed()) {
                    logger.error("", deploy.cause());
                    testContext.fail("Could not deploy verticle");
                    async.complete();
                    vertx.close();
                } else {
                    // have the test run for 20 seconds to give you enough time to get a message off
                    long timerId = vertx.setTimer(60000, theTimerId ->
                    {
                        logger.info("Failed to get any messages");
                        testContext.fail("Test did not complete in 20 seconds");
                        async.complete();
                        vertx.close();
                    });

                    logger.info("Registering listener on event bus for kafka messages");

                    vertx.eventBus().consumer(SimpleConsumer.EVENTBUS_DEFAULT_ADDRESS, (Message<JsonObject> message) -> {
                        assertTrue(message.body().toString().length() > 0);
                        logger.info("got message: " + message.body());
                        KafkaEvent event = new KafkaEvent(message.body());
                        SyslogSinkConsumer consume = new SyslogSinkConsumer();
                        try {
                        	consume.setSyslogdConfig(getConfig());
							consume.handleMessage(getSyslogMsg());
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
                        vertx.cancelTimer(timerId);
                        async.complete();
                        vertx.close();
                    });
                }
            }
        );
    }
    
    protected SyslogMessageLogDTO getSyslogMsg() throws UnknownHostException
    {
    	byte[] bytes = "<31>main: 2017-10-03 localhost foo%d: load testpavan %d on tty1".getBytes();
    	DatagramPacket pkt = new DatagramPacket(bytes, bytes.length, InetAddress.getLocalHost(), 5140);
    	SyslogConnection con = new SyslogConnection(pkt, false);
    	final SyslogMessageLogDTO messageLog = new SyslogMessageLogDTO("LOC1", "SYSID1",con.getSource());
    	SyslogMessageDTO syslogMsgDto = new SyslogMessageDTO();
    	ByteBuffer message = ByteBuffer.wrap(bytes);
    	syslogMsgDto.setBytes(message);
    	List<SyslogMessageDTO> messageDTOs = new ArrayList<SyslogMessageDTO>();
    	messageDTOs.add(syslogMsgDto);
    	messageLog.setMessages(messageDTOs);
    	return messageLog;
    }
    
    protected SyslogdConfig getConfig() throws IOException
    {
    	// 10000 sample syslogmessages from xml file are taken and passed as
		InputStream stream = ConfigurationTestUtils.getInputStreamForResource(this,
				"/etc/syslogd-loadtest-configuration.xml");
		// Inputstream to create syslogdconfiguration
		// InputStream stream = new FileInputStream(
		// "/Users/ms043660/OneDrive - Cerner
		// Corporation/Office/ProjectWorkspace/POC/Vertex/syslog/src/test/resources/etc/syslogd-loadtest-configuration.xml");
		SyslogdConfig config = new SyslogdConfigFactory(stream);
		return config;
    }
    
}