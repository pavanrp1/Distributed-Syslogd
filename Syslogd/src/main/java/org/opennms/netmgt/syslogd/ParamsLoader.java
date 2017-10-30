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
import org.opennms.core.test.ConfigurationTestUtils;
import org.opennms.core.utils.ConfigFileConstants;
import org.opennms.netmgt.config.SyslogdConfigFactory;
import org.opennms.netmgt.dao.hibernate.DistPollerDaoHibernate;
import org.opennms.netmgt.syslogd.BufferParser.BufferParserFactory;
import org.opennms.netmgt.syslogd.api.SyslogMessageLogDTO;
import org.opennms.netmgt.xml.event.Parm;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;

public class ParamsLoader extends AbstractVerticle {

	private static List<String> grokPatternsList;
	private static SyslogdConfigFactory syslogdConfig;

	public static List<String> getGrokPatternsList() {
		return grokPatternsList;
	}

	private final static ExecutorService m_executor = Executors.newSingleThreadExecutor();

	/**
	 * Static block to load grokPatterns during the start of SyslogSink class call.
	 */
	static {
		try {
			grokPatternsList = readPropertiesInOrderFrom(
					ConfigFileConstants.getFile(ConfigFileConstants.SYSLOGD_CONFIGURATION_PROPERTIES));
			syslogdConfig = loadSyslogConfiguration("/etc/syslogd-loadtest-configuration.xml");
		} catch (IOException e) {
		}

	}

	private Map<String, String> paramsMap;

	public ParamsLoader() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public void start(Future<Void> startFuture) throws Exception {

		io.vertx.core.eventbus.MessageConsumer<SyslogMessageLogDTO> consumerFromEventBus = vertx.eventBus()
				.consumer("eventd.message.consumer");
		consumerFromEventBus.handler(syslogDTOMessage -> {
			parse(syslogDTOMessage.body().getMessages().get(0).getBytes());
			syslogDTOMessage.body().setParamsMap(paramsMap);
			syslogDTOMessage.body().setSyslogdConfig(syslogdConfig);
			vertx.eventBus().send("parms.message.consumer", syslogDTOMessage.body());
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

	private static SyslogdConfigFactory loadSyslogConfiguration(final String configuration) throws IOException {
		InputStream stream = null;
		try {
			stream = ConfigurationTestUtils.getInputStreamForResource(new SyslogSinkConsumer(), configuration);
			return new SyslogdConfigFactory(stream);
		} finally {
			if (stream != null) {
				IOUtils.closeQuietly(stream);
			}
		}
	}

	public static void loadGrokParserList() throws IOException {
		grokPatternsList = new ArrayList<String>();
		File syslogConfigFile = ConfigFileConstants.getFile(ConfigFileConstants.SYSLOGD_CONFIGURATION_PROPERTIES);
		readPropertiesInOrderFrom(syslogConfigFile);
	}

}
