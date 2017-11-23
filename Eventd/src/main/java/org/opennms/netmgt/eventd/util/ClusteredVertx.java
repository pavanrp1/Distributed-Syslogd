package org.opennms.netmgt.eventd.util;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.function.Consumer;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.eventbus.EventBusOptions;
import io.vertx.core.spi.cluster.ClusterManager;
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
		hazelcastConfig.getGroupConfig().setName("group1");
		hazelcastConfig.getNetworkConfig().getJoin().getTcpIpConfig().addMember(ConfigConstants.LOCALHOST).setEnabled(true);
		// //
		// hazelcastConfig.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
		hazelcastConfig.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
		//
		// hazelcastConfig.getNetworkConfig().getJoin().getAwsConfig().setEnabled(false);
		// hazelcastConfig.getGroupConfig().setName("group1");
		// JoinConfig join = hazelcastConfig.getNetworkConfig().getJoin();
		// join.getMulticastConfig().setEnabled(false);
		// join.getAwsConfig().setEnabled(false);
		// join.getTcpIpConfig().setEnabled(true).setMembers(Arrays.asList("10.182.247.73",
		// "10.182.241.200"));
		// hazelcastConfig.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("10.182.*.*");
		ClusterManager mgr = new HazelcastClusterManager(hazelcastConfig);

		VertxOptions vertxOptions = new VertxOptions().setClustered(true).setClusterManager(mgr);
		InetAddress IP;
		try {
			IP = InetAddress.getLocalHost();
			vertxOptions.setClusterHost(IP.getHostAddress());
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// vertxOptions.setEventBusOptions(new
		// EventBusOptions().setClustered(true).setClusterPublicHost("application"));

		runnerWithVertxClustered(CORE_EXAMPLES_JAVA_DIR, clazz, vertxOptions, options);
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
