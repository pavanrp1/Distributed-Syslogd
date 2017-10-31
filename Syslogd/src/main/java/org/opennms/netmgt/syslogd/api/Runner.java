package org.opennms.netmgt.syslogd.api;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;

import java.io.File;
import java.io.IOException;
import java.util.function.Consumer;

import org.opennms.netmgt.xml.event.Log;

/*
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
public class Runner {

	private static final String CORE_EXAMPLES_DIR = "core-examples";
	private static final String CORE_EXAMPLES_JAVA_DIR = CORE_EXAMPLES_DIR + "/src/main/java/";

	public static void runClusteredExample(Class clazz) {
		runExample(CORE_EXAMPLES_JAVA_DIR, clazz, new VertxOptions().setClustered(true), null);
	}

	public static void runClusteredExample(Class clazz, VertxOptions options) {
		runExample(CORE_EXAMPLES_JAVA_DIR, clazz, options.setClustered(true), null);
	}

	public static void runExample(Class clazz) {
		runExample(CORE_EXAMPLES_JAVA_DIR, clazz, new VertxOptions().setClustered(false), null);
	}

	public static void runExample(Class clazz, DeploymentOptions options) {
		runExample(CORE_EXAMPLES_JAVA_DIR, clazz, new VertxOptions().setClustered(false), options);
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
					vertx.eventBus().registerDefaultCodec(Log.class, new SyslogdMessageCodec());
					vertx.eventBus().registerDefaultCodec(SyslogMessageLogDTO.class, new SyslogdDTOMessageCodec());
					runner.accept(vertx);
				} else {
					res.cause().printStackTrace();
				}
			});
		} else {
			Vertx vertx = Vertx.vertx(options);
			vertx.eventBus().registerDefaultCodec(Log.class, new SyslogdMessageCodec());
			vertx.eventBus().registerDefaultCodec(SyslogMessageLogDTO.class, new SyslogdDTOMessageCodec());
			runner.accept(vertx);
		}
	}
}
