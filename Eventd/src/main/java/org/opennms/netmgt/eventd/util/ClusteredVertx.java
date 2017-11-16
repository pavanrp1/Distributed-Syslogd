package org.opennms.netmgt.eventd.util;

import java.io.File;
import java.io.IOException;
import java.util.function.Consumer;

import com.hazelcast.config.Config;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.ext.dropwizard.DropwizardMetricsOptions;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;

public class ClusteredVertx {

	private static final String CORE_EXAMPLES_JAVA_DIR = "/src/main/java/";

	public static void runClustered(Class<?> clazz) {
		runnerWithVertxClustered(CORE_EXAMPLES_JAVA_DIR, clazz, new VertxOptions().setClustered(true), null);
	}

	public static void runClustered(Class<?> clazz, VertxOptions options) {
		runnerWithVertxClustered(CORE_EXAMPLES_JAVA_DIR, clazz, options.setClustered(true), null);
	}

	public static void runClusteredWithDeploymentOptions(Class<?> clazz, DeploymentOptions options) {
		Config hazelcastConfig = new Config();
		hazelcastConfig.getNetworkConfig().getJoin().getTcpIpConfig().addMember(ConfigConstants.LOCALHOST)
				.setEnabled(true);
		hazelcastConfig.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);

		ClusterManager mgr = new HazelcastClusterManager(hazelcastConfig);
		runnerWithVertxClustered(CORE_EXAMPLES_JAVA_DIR, clazz,
				new VertxOptions().setClustered(true).setClusterManager(mgr), options);
	}

	public static void runClusteredWithDeploymentOptions(Class<?> clazz, DeploymentOptions options, String domainName) {
		Config hazelcastConfig = new Config();
		hazelcastConfig.getNetworkConfig().getJoin().getTcpIpConfig().addMember(ConfigConstants.LOCALHOST)
				.setEnabled(true);
		hazelcastConfig.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);

		ClusterManager mgr = new HazelcastClusterManager(hazelcastConfig);
		runnerWithVertxClustered(CORE_EXAMPLES_JAVA_DIR, clazz,
				new VertxOptions().setClustered(true).setMetricsOptions(
						new DropwizardMetricsOptions().setEnabled(true).setJmxEnabled(true).setJmxDomain(domainName)),
				options);
	}

	public static void runnerWithVertxClustered(String exampleDir, Class<?> clazz, VertxOptions options,
			DeploymentOptions deploymentOptions) {
		clusteredVertx(exampleDir + clazz.getPackage().getName().replace('.', '/'), clazz.getName(), options,
				deploymentOptions);
	}

	public static void runScriptExample(String prefix, String scriptName, VertxOptions options) {
		File file = new File(scriptName);
		String dirPart = file.getParent();
		String scriptDir = prefix + dirPart;
		clusteredVertx(scriptDir, scriptDir + '/' + file.getName(), options, null);
	}

	public static void clusteredVertx(String exampleDir, String verticleID, VertxOptions options,
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
					runner.accept(vertx);
				} else {
					res.cause().printStackTrace();
				}
			});
		} else {
			Vertx vertx = Vertx.vertx(options);
			runner.accept(vertx);
		}
	}
}
