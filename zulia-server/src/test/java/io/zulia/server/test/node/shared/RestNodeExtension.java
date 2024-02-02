package io.zulia.server.test.node.shared;

import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.rest.ZuliaRESTClient;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestNodeExtension implements BeforeAllCallback, AfterAllCallback, BeforeEachCallback, AfterEachCallback {
	private final static Logger LOG = LoggerFactory.getLogger(RestNodeExtension.class);
	private final int nodeCount;

	private ZuliaWorkPool zuliaWorkPool;
	private ZuliaRESTClient zuliaRestClient;

	public RestNodeExtension(int nodeCount) {
		this.nodeCount = nodeCount;
	}

	public ZuliaWorkPool getGrpcClient() {
		return zuliaWorkPool;
	}

	public ZuliaRESTClient getRESTClient() {
		return zuliaRestClient;
	}

	@Override
	public void beforeAll(ExtensionContext context) throws Exception {
		if (context.getTestClass().isPresent()) {
			LOG.info("Suite started: " + context.getTestClass().get());
		}
		TestHelper.createNodes(nodeCount);
		TestHelper.startNodes(true);
		Thread.sleep(2000);
		zuliaWorkPool = TestHelper.createClient();
		zuliaRestClient = TestHelper.createRESTClient();

	}

	public void afterAll(ExtensionContext context) throws Exception {
		if (context.getTestClass().isPresent()) {
			LOG.info("Suite finishing: " + context.getTestClass().get());
		}
		TestHelper.stopNodes();
		zuliaWorkPool.shutdown();
		zuliaRestClient.close();
		if (context.getTestClass().isPresent()) {
			LOG.info("Suite finished: " + context.getTestClass().get());
		}

	}

	@Override
	public void beforeEach(ExtensionContext context) throws Exception {
		LOG.info("Test started: " + context.getDisplayName());

	}

	@Override
	public void afterEach(ExtensionContext context) throws Exception {
		LOG.info("Test finished: " + context.getDisplayName());
	}

}
