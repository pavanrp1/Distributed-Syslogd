package org.opennms.netmgt.syslogd;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.StringWriter;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.xml.bind.JAXBException;

import org.eclipse.persistence.jaxb.JAXBUnmarshaller;
import org.opennms.core.utils.ConfigFileConstants;
import org.opennms.core.xml.XmlHandler;
import org.opennms.netmgt.config.SyslogdConfigFactory;
import org.opennms.netmgt.syslogd.BufferParser.BufferParserFactory;
import org.opennms.netmgt.syslogd.api.Runner;
import org.opennms.netmgt.syslogd.api.SyslogMessageLogDTO;
import org.opennms.netmgt.syslogd.api.UtilMarshler;
import org.opennms.netmgt.xml.event.Parm;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.eventbus.EventBus;

public class ParamsLoader extends AbstractVerticle {

	private ExecutorService backgroundConsumer;

	private static List<String> grokPatternsList;

	private static UtilMarshler utilMarshler;

	public void setGrokPatternsList(List<String> grokPatternsList) {
		ParamsLoader.grokPatternsList = grokPatternsList;
	}

	private static SyslogdConfigFactory syslogdConfig;

	private static Gson gson;

	public static List<String> getGrokPatternsList() {
		return grokPatternsList;
	}

	private final static ExecutorService m_executor = Executors.newSingleThreadExecutor();

	private static final String EVENTD_CONSUMER_ADDRESS = "eventd.message.consumer";

	public static void main(String[] args) {
		SyslogTimeStamp.broadcastCount = new AtomicInteger();
		System.setProperty("opennms.home", "src/test/resources");
		try {
			grokPatternsList = readPropertiesInOrderFrom(
					ConfigFileConstants.getFile(ConfigFileConstants.SYSLOGD_CONFIGURATION_PROPERTIES));
		} catch (IOException e) {
			e.printStackTrace();
		}
		DeploymentOptions deployment = new DeploymentOptions();
		deployment.setWorker(true);
		deployment.setWorkerPoolSize(Integer.MAX_VALUE);
		deployment.setMultiThreaded(true);
		// deployment.setInstances(100);
		// gson = new GsonBuilder().registerTypeAdapter(ByteBuffer.class, new
		// ByteBufferXmlAdapter()).create();
		Runner.runClusteredExample1(ParamsLoader.class, deployment);
	}

	private static Map<String, String> paramsMap;

	public Map<String, String> getParamsMap() {
		return paramsMap;
	}

	public static void setParamsMap(Map<String, String> paramsMap) {
		ParamsLoader.paramsMap = paramsMap;
	}

	private EventBus syslogdEventbus;

	public ParamsLoader() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public void start() throws Exception {
		utilMarshler = new UtilMarshler(SyslogMessageLogDTO.class);
		syslogdEventbus = vertx.eventBus();
		backgroundConsumer = Executors.newSingleThreadExecutor();
		backgroundConsumer.submit(() -> {
			vertx.eventBus().consumer("eventd.message.consumer", e -> {

				SyslogMessageLogDTO syslogMessage = utilMarshler.unmarshal((String) e.body());
				syslogMessage.setParamsMap(parse(syslogMessage.getMessages().getBytes()));
				vertx.eventBus().send("parms.message.consumer", utilMarshler.marshal(syslogMessage));
				// System.out.println("At Params " +
				// SyslogTimeStamp.broadcastCount.incrementAndGet());
			});

		});
	}

	private SyslogMessageLogDTO getSyslogDto(String body) {

		return gson.fromJson(body, SyslogMessageLogDTO.class);
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
