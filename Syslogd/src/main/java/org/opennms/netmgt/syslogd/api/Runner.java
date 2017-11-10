package org.opennms.netmgt.syslogd.api;

import java.io.File;
import java.io.IOException;
import java.util.function.Consumer;

import org.opennms.netmgt.xml.event.Log;

import com.hazelcast.config.Config;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;

/*
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
public class Runner {

	private static final String CORE_EXAMPLES_DIR = "core-examples";
	private static final String CORE_EXAMPLES_JAVA_DIR = CORE_EXAMPLES_DIR + "/src/main/java/";

	public static void runClusteredExample(Class clazz) {
		runExample(CORE_EXAMPLES_JAVA_DIR, clazz, new VertxOptions().setClustered(true), null);
		// runExample(CORE_EXAMPLES_JAVA_DIR, clazz, new
		// VertxOptions().setClustered(true).setClusterManager(clusterManager), null);
	}

	public static void runClusteredExample(Class clazz, VertxOptions options) {
		runExample(CORE_EXAMPLES_JAVA_DIR, clazz, options.setClustered(true), null);
	}

	public static void runExample(Class clazz) {
		runExample(CORE_EXAMPLES_JAVA_DIR, clazz, new VertxOptions().setClustered(false), null);
	}

	public static void runClusteredExample(Class clazz, DeploymentOptions options) {
		// Config hazelcastConfig = new Config();
		// hazelcastConfig.getNetworkConfig().getJoin().getTcpIpConfig().addMember("127.0.0.1").setEnabled(true);
		// hazelcastConfig.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
		// ClusterManager mgr = new JGroupsClusterManager();
		runExample(CORE_EXAMPLES_JAVA_DIR, clazz, new VertxOptions().setClustered(true).setClusterHost("127.0.0.1"),
				options);
	}

	public static void runClusteredExample1(Class clazz, DeploymentOptions options) {

		Config hazelcastConfig = new Config();
		hazelcastConfig.getNetworkConfig().getJoin().getTcpIpConfig().addMember("127.0.0.1").setEnabled(true);
		hazelcastConfig.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);

		ClusterManager mgr = new HazelcastClusterManager(hazelcastConfig);
		runExample(CORE_EXAMPLES_JAVA_DIR, clazz, new VertxOptions().setClustered(true).setClusterManager(mgr),
				options);
	}

	public static void runExample(String exampleDir, Class clazz, VertxOptions options,
			DeploymentOptions deploymentOptions) {
		runExample(exampleDir + clazz.getPackage().getName().replace(".", "/"), clazz.getName(), options,
				deploymentOptions);
	}

	public static void runScriptExample(String prefix, String scriptName, VertxOptions options) {
		File file = new File(scriptName);
		String dirPart = file.getParent();
		String scriptDir = prefix + dirPart;
		runExample(scriptDir, scriptDir + "/" + file.getName(), options, null);
	}

	public static void runExample(String exampleDir, String verticleID, VertxOptions options,
			DeploymentOptions deploymentOptions) {
		if (options == null) {
			options = new VertxOptions();
		}
		try {
			File current = new File(".").getCanonicalFile();
			if (exampleDir.startsWith(current.getName()) && !exampleDir.equals(current.getName())) {
				exampleDir = exampleDir.substring(current.getName().length() + 1);
			}
		} catch (IOException e) {
		}

		System.setProperty("vertx.cwd", exampleDir);
		Consumer<Vertx> runner = vertx -> {
			try {
				if (deploymentOptions != null) {
					vertx.deployVerticle(verticleID, deploymentOptions);
				} else {
					vertx.deployVerticle(verticleID);
				}
			} catch (Throwable t) {
				t.printStackTrace();
			}
		};
		if (options.isClustered()) {
			Vertx.clusteredVertx(options, res -> {
				if (res.succeeded()) {
					Vertx vertx = res.result();
					// vertx.eventBus().registerDefaultCodec(Log.class, new SyslogdMessageCodec());
					// vertx.eventBus().registerDefaultCodec(StringBuilder.class, new Builder());
					// vertx.eventBus().registerDefaultCodec(SyslogMessageLogDTO.class, new
					// SyslogdDTOMessageCodec());
					runner.accept(vertx);
				} else {
					res.cause().printStackTrace();
				}
			});
		} else {
			Vertx vertx = Vertx.vertx(options);
			// vertx.eventBus().registerDefaultCodec(StringBuilder.class, new Builder());
			// vertx.eventBus().registerDefaultCodec(Log.class, new SyslogdMessageCodec());
			// vertx.eventBus().registerDefaultCodec(SyslogMessageLogDTO.class, new
			// SyslogdDTOMessageCodec());
			runner.accept(vertx);
		}
	}
}
