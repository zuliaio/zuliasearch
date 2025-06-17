package io.zulia.server.test.node.shared;

import io.zulia.client.pool.ZuliaWorkPool;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodeExtension implements BeforeAllCallback, AfterAllCallback, BeforeEachCallback, AfterEachCallback {
	private final static Logger LOG = LoggerFactory.getLogger(NodeExtension.class);
	private final int nodeCount;

	public NodeExtension(int nodeCount) {
		this.nodeCount = nodeCount;
	}

	private ZuliaWorkPool zuliaWorkPool;

	public ZuliaWorkPool getClient() {
		return zuliaWorkPool;
	}

	@Override
	public void beforeAll(ExtensionContext context) throws Exception {
		if (context.getTestClass().isPresent()) {
			LOG.info("Suite started: {}", context.getTestClass().get());
		}
		TestHelper.createNodes(nodeCount);
		TestHelper.startNodes(false);
		Thread.sleep(2000);
		zuliaWorkPool = TestHelper.createClient();
	}

	public void afterAll(ExtensionContext context) throws Exception {
		if (context.getTestClass().isPresent()) {
			LOG.info("Suite finishing: {}", context.getTestClass().get());
		}
		TestHelper.stopNodes();
		zuliaWorkPool.close();
		if (context.getTestClass().isPresent()) {
			LOG.info("Suite finished: {}", context.getTestClass().get());
		}
	}

	@Override
	public void beforeEach(ExtensionContext context) throws Exception {
		LOG.info("Test started: {}", context.getDisplayName());

	}

	@Override
	public void afterEach(ExtensionContext context) throws Exception {
		LOG.info("Test finished: {}", context.getDisplayName());
	}

	public void restartNodes() throws Exception {
		TestHelper.stopNodes();
		Thread.sleep(2000);
		TestHelper.startNodes(false); // micronaut does not like starting again on the same port
		Thread.sleep(2000);
	}
}
