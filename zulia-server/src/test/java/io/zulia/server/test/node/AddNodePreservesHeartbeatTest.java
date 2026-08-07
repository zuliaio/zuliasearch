package io.zulia.server.test.node;

import io.zulia.message.ZuliaBase.Node;
import io.zulia.server.config.cluster.MongoNodeService;
import io.zulia.server.test.node.shared.NodeExtension;
import io.zulia.server.test.node.shared.TestHelper;
import io.zulia.server.util.MongoProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * addNode used replaceOne with a document that carries no heartbeat field, so re-registering an already
 * running node erased its heartbeat, and every peer expelled the healthy node within a second through the
 * clean-shutdown path, which bypasses the expiry debounce entirely. Registration must only set the
 * registration fields.
 */
public class AddNodePreservesHeartbeatTest {

	@RegisterExtension
	static final NodeExtension nodeExtension = new NodeExtension(1);

	@Test
	public void reRegistrationPreservesHeartbeat() throws Exception {
		MongoNodeService nodeService = new MongoNodeService(MongoProvider.getMongoClient(),
				TestHelper.getZuliaNodes().getFirst().getZuliaConfig().getClusterName());

		// a fabricated peer so the live node's 1-second heartbeat ticks cannot mask a wipe
		Node fake = Node.newBuilder().setServerAddress("fakehost").setServicePort(12345).setRestPort(12346).build();
		nodeService.addNode(fake);
		nodeService.updateHeartbeat("fakehost", 12345);
		Assertions.assertTrue(heartbeatOf(nodeService, "fakehost", 12345) > 0, "heartbeat must be set after updateHeartbeat");

		nodeService.addNode(fake);
		Assertions.assertTrue(heartbeatOf(nodeService, "fakehost", 12345) > 0,
				"re-registration must preserve the heartbeat, or every peer expels the node as cleanly shut down");

		nodeService.removeNode("fakehost", 12345);
	}

	private long heartbeatOf(MongoNodeService nodeService, String serverAddress, int servicePort) throws Exception {
		for (Node node : nodeService.getNodes()) {
			if (node.getServerAddress().equals(serverAddress) && node.getServicePort() == servicePort) {
				return node.getHeartbeat();
			}
		}
		Assertions.fail("node " + serverAddress + ":" + servicePort + " not found in the registry");
		return 0;
	}
}
