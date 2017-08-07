package io.zulia.server.rest;

import io.zulia.server.config.ZuliaConfig;
import io.zulia.server.index.ZuliaIndexManager;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class ZuliaRESTServiceManager {

	private static final Logger LOG = Logger.getLogger(ZuliaRESTServiceManager.class.getSimpleName());

	private final int restPort;
	private final ZuliaIndexManager indexManager;

	private HttpServer server;

	public ZuliaRESTServiceManager(ZuliaConfig zuliaConfig, ZuliaIndexManager indexManager) {
		this.restPort = zuliaConfig.getRestPort();
		this.indexManager = indexManager;
	}

	public void start() {
		URI baseUri = UriBuilder.fromUri("http://0.0.0.0/").port(restPort).build();
		ResourceConfig config = new ResourceConfig();
		config.register(new AssociatedResource(indexManager));
		config.register(new QueryResource(indexManager));
		config.register(new FetchResource(indexManager));
		config.register(new FieldsResource(indexManager));
		config.register(new IndexResource(indexManager));
		config.register(new IndexesResource(indexManager));
		config.register(new TermsResource(indexManager));
		config.register(new NodesResource(indexManager));
		config.register(new StatsResource(indexManager));
		server = GrizzlyHttpServerFactory.createHttpServer(baseUri, config);
		server.getListener("grizzly").setMaxHttpHeaderSize(128 * 1024);
	}

	public void shutdown() {
		LOG.info("Starting rest service shutdown");
		//TODO configure
		server.shutdown(1, TimeUnit.SECONDS);
	}
}
