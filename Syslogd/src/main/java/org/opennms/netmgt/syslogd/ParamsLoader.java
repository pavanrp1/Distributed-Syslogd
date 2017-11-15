package org.opennms.netmgt.syslogd;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
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
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.opennms.core.utils.ConfigFileConstants;
import org.opennms.core.xml.XmlHandler;
import org.opennms.netmgt.config.SyslogdConfigFactory;
import org.opennms.netmgt.eventd.util.ClusteredVertx;
import org.opennms.netmgt.eventd.util.ConfigConstants;
import org.opennms.netmgt.eventd.util.UtiliMarshlerUnmarshaler;
import org.opennms.netmgt.syslogd.BufferParser.BufferParserFactory;
import org.opennms.netmgt.syslogd.api.SyslogMessageLogDTO;
import org.opennms.netmgt.xml.event.Parm;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.eventbus.EventBus;

public class ParamsLoader extends AbstractVerticle {

	private ExecutorService backgroundConsumer;

	private static List<String> grokPatternsList;

	private static UtiliMarshlerUnmarshaler xmlHandler;

	private SyslogMessageLogDTO syslogMessageLogDTO;

	private final static ExecutorService m_executor = Executors.newSingleThreadExecutor();

	static {
		try {
			System.setProperty(ConfigConstants.OPENNMS_HOME, "src/test/resources");
			grokPatternsList = readPropertiesInOrderFrom(
					ConfigFileConstants.getFile(ConfigFileConstants.SYSLOGD_CONFIGURATION_PROPERTIES));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void setGrokPatternsList(List<String> grokPatternList) {
		grokPatternsList = grokPatternList;
	}

	public static List<String> getGrokPatternsList() {
		return grokPatternsList;
	}

	public static void main(String[] args) throws Exception {
		ClusteredVertx.runClusteredWithDeploymentOptions(ParamsLoader.class, new DeploymentOptions(), true);
	}

	private static Map<String, String> paramsMap;

	public Map<String, String> getParamsMap() {
		return paramsMap;
	}

	public static void setParamsMap(Map<String, String> messageParamsMap) {
		paramsMap = messageParamsMap;
	}

	private EventBus paramsEventBus;

	public ParamsLoader() {
	}

	@Override
	public void start() throws Exception {
		xmlHandler = new UtiliMarshlerUnmarshaler(SyslogMessageLogDTO.class);
		paramsEventBus = vertx.eventBus();
		backgroundConsumer = Executors.newSingleThreadExecutor();
		backgroundConsumer.submit(() -> {
			paramsEventBus.consumer(ConfigConstants.KAFKA_CONSUMER_ADDRESS, syslogMessageDTO -> {
				syslogMessageLogDTO = (SyslogMessageLogDTO) xmlHandler.unmarshal((String) syslogMessageDTO.body());
				syslogMessageLogDTO.setParamsMap(parse(syslogMessageLogDTO.getMessages().getBytes()));
				paramsEventBus.send(ConfigConstants.CONVERT_TO_EVENT_CONSUMER_ADDRESS,
						xmlHandler.marshal(syslogMessageLogDTO));
			});

		});
	}

	/**
	 * This method will parse the message against the grok patterns
	 * 
	 * @param messageBytes
	 * @return
	 * 
	 * @return Parameter list
	 */
	public Map<String, String> parse(ByteBuffer messageBytes) {
		String grokPattern;
		paramsMap = new HashMap<String, String>();
		if (null == getGrokPatternsList() || getGrokPatternsList().isEmpty()) {
			System.out.println("error");
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
		return paramsMap;

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

	public void loadGrokParserList() throws IOException {
		grokPatternsList = new ArrayList<String>();
		File syslogConfigFile = ConfigFileConstants.getFile(ConfigFileConstants.SYSLOGD_CONFIGURATION_PROPERTIES);
		readPropertiesInOrderFrom(syslogConfigFile);
	}

}
