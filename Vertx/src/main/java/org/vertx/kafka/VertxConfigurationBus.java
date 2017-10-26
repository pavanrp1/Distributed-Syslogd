package org.vertx.kafka;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.vertx.kafka.util.VertxConfiguration;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;

public class VertxConfigurationBus extends AbstractVerticle {

	private VertxConfiguration vertxConfiguration;

	private static final String CONFIGURATION_CONSUMER_ADDRESS = "configuration.message.consumer";

	public VertxConfiguration getVertxConfiguration() {
		return vertxConfiguration;
	}

	public void setVertxConfiguration(VertxConfiguration vertxConfiguration) {
		this.vertxConfiguration = vertxConfiguration;
	}

	private Map<String, JsonObject> products = new HashMap<>();

	private AtomicBoolean concurrentRunning;

	@Override
	public void start(Future<Void> startFuture) throws Exception {

		setUpInitialData();

		Router router = Router.router(vertx);

		router.route().handler(BodyHandler.create());
		router.get("/products/:productID").handler(this::handleGetProduct);
		router.put("/products/:productID").handler(this::handleAddProduct);
		router.get("/products").handler(this::handleListProducts);

		vertx.createHttpServer().requestHandler(router::accept).listen(8080);

		HttpClient httpClient = vertx.createHttpClient();
		httpClient.getNow(8080, "localhost", "/", new Handler<HttpClientResponse>() {

			@Override
			public void handle(HttpClientResponse httpClientResponse) {
				httpClientResponse.bodyHandler(msg -> {
					System.out.println(msg);
				});
			}
		});

	}

	private void handleGetProduct(RoutingContext routingContext) {
		String productID = routingContext.request().getParam("productID");
		HttpServerResponse response = routingContext.response();
		if (productID == null) {
			sendError(400, response);
		} else {
			JsonObject product = products.get(productID);
			if (product == null) {
				sendError(404, response);
			} else {
				response.putHeader("content-type", "application/json").end(product.encodePrettily());
			}
		}
	}

	private void handleAddProduct(RoutingContext routingContext) {
		String productID = routingContext.request().getParam("productID");
		HttpServerResponse response = routingContext.response();
		if (productID == null) {
			sendError(400, response);
		} else {
			JsonObject product = routingContext.getBodyAsJson();
			if (product == null) {
				sendError(400, response);
			} else {
				products.put(productID, product);
				response.end();
			}
		}
	}

	private void handleListProducts(RoutingContext routingContext) {
		JsonArray arr = new JsonArray();
		products.forEach((k, v) -> arr.add(v));
		routingContext.response().putHeader("content-type", "application/json").end(arr.encodePrettily());
	}

	private void sendError(int statusCode, HttpServerResponse response) {
		response.setStatusCode(statusCode).end();
	}

	private void setUpInitialData() {
		addProduct(new JsonObject().put("id", "configuration").put("name", getVertxConfiguration()));
	}

	private void addProduct(JsonObject product) {
		products.put(product.getString("id"), product);
	}

}
