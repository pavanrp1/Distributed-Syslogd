/**
 * 
 */
package org.Vertx.kafka.util;

import java.io.IOException;
import java.io.InputStream;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.opennms.core.test.ConfigurationTestUtils;
import org.opennms.netmgt.config.SyslogdConfig;
import org.opennms.netmgt.config.SyslogdConfigFactory;
import org.opennms.netmgt.syslogd.api.SyslogConnection;
import org.opennms.netmgt.syslogd.api.SyslogMessageDTO;
import org.opennms.netmgt.syslogd.api.SyslogMessageLogDTO;

/**
 * @author pk015603
 *
 */
public class DistributedUtil {
	
	/**
	 * This method will give you back the example @SyslogMessageLogDTO
	 * 
	 * @return
	 * @throws UnknownHostException
	 */
	public static SyslogMessageLogDTO getSyslogMsg() throws UnknownHostException
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
    
	/**
	 * This method will give you the config file 
	 * @return
	 * @throws IOException
	 */
    public static SyslogdConfig getConfig(Object obj) throws IOException
    {
    	// 10000 sample syslogmessages from xml file are taken and passed as
		InputStream stream = ConfigurationTestUtils.getInputStreamForResource(obj,
				"/etc/syslogd-loadtest-configuration.xml");
		// Inputstream to create syslogdconfiguration
		// InputStream stream = new FileInputStream(
		// "/Users/ms043660/OneDrive - Cerner
		// Corporation/Office/ProjectWorkspace/POC/Vertex/syslog/src/test/resources/etc/syslogd-loadtest-configuration.xml");
		SyslogdConfig config = new SyslogdConfigFactory(stream);
		return config;
    }

}
