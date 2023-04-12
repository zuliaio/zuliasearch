package io.zulia.server.test.node.shared;

import io.zulia.client.pool.ZuliaWorkPool;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public class NodeExtension implements BeforeAllCallback, AfterAllCallback, BeforeEachCallback, AfterEachCallback {

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
			System.out.println("Suite started: " + context.getTestClass().get());
		}
		TestHelper.createNodes(nodeCount);
		TestHelper.startNodes();
		Thread.sleep(2000);
		zuliaWorkPool = TestHelper.createClient();
	}

	public void afterAll(ExtensionContext context) throws Exception {
		if (context.getTestClass().isPresent()) {
			System.out.println("Suite finishing: " + context.getTestClass().get());
		}
		TestHelper.stopNodes();
		zuliaWorkPool.shutdown();
		if (context.getTestClass().isPresent()) {
			System.out.println("Suite finished: " + context.getTestClass().get());
		}
	}

	@Override
	public void beforeEach(ExtensionContext context) throws Exception {
		System.out.println("Test started: " + context.getDisplayName());

	}

	@Override
	public void afterEach(ExtensionContext context) throws Exception {
		System.out.println("Test finished: " + context.getDisplayName());
	}

	public void restartNodes() throws Exception {
		TestHelper.stopNodes();
		Thread.sleep(2000);
		TestHelper.startNodes();
		Thread.sleep(2000);
	}
}
