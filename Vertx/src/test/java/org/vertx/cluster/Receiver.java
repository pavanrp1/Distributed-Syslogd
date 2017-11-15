/**
 * 
 */
package org.vertx.cluster;

import org.opennms.netmgt.eventd.Runner;

/**
 * @author pk015603
 *
 */
import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.EventBus;

/*
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
public class Receiver extends AbstractVerticle {

	// Convenience method so you can run it in your IDE
	public static void main(String[] args) {
		Runner.runClusteredExample(Receiver.class);
	}

	@Override
	public void start() throws Exception {

		EventBus eb = vertx.eventBus();

		eb.consumer("news-feed", message -> System.out.println("Received news on consumer 1: " + message.body()));

		// eb.consumer("news-feed", message -> System.out.println("Received news on
		// consumer 2: " + message.body()));
		//
		// eb.consumer("news-feed", message -> System.out.println("Received news on
		// consumer 3: " + message.body()));

		System.out.println("Ready!");
	}
}
