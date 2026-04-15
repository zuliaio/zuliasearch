package io.zulia.client.pool;

import io.zulia.client.command.base.BaseCommand;
import io.zulia.client.command.base.GrpcCommand;
import io.zulia.client.config.ZuliaPoolConfig;
import io.zulia.client.pool.stub.StubGrpcCommand;
import io.zulia.client.pool.stub.StubMultiIndexCommand;
import io.zulia.client.pool.stub.StubResult;
import io.zulia.client.pool.stub.StubShardCommand;
import io.zulia.client.pool.stub.StubSingleIndexCommand;
import io.zulia.client.result.Result;
import io.zulia.message.ZuliaBase.Node;
import io.zulia.message.ZuliaIndex.IndexShardMapping;
import io.zulia.message.ZuliaIndex.ShardMapping;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class ZuliaPoolTest {

	private ZuliaPool pool;

	@AfterEach
	void tearDown() {
		if (pool != null) {
			pool.close();
		}
	}

	private Node makeNode(String host, int port) {
		return Node.newBuilder().setServerAddress(host).setServicePort(port).setRestPort(port + 1).build();
	}

	private ZuliaPoolConfig configWithNodes(Node... nodes) {
		var config = new ZuliaPoolConfig();
		config.setNodeUpdateEnabled(false);
		config.setRoutingEnabled(false);
		config.setConnectionListener(null);
		for (var node : nodes) {
			config.addNode(node);
		}
		return config;
	}

	// Random node fallback 

	@Test
	void executePicksRandomNodeWhenRoutingDisabled() throws Exception {
		var node = makeNode("localhost", 32191);
		var config = configWithNodes(node);
		pool = new ZuliaPool(config);

		var result = pool.execute(new StubGrpcCommand());
		assertEquals("localhost", result.getNodeAddress());
	}

	@Test
	void executeThrowsWhenNoNodes() {
		var config = configWithNodes();
		pool = new ZuliaPool(config);

		assertThrows(IOException.class, () -> pool.execute(new StubGrpcCommand()));
	}

	// Routing dispatch 

	@Test
	void shardRoutableCommandRoutesToCorrectNode() throws Exception {
		var nodeA = makeNode("hostA", 32191);
		var nodeB = makeNode("hostB", 32191);

		var config = new ZuliaPoolConfig();
		config.setNodeUpdateEnabled(false);
		config.setRoutingEnabled(true);
		config.setConnectionListener(null);
		config.addNode(nodeA);
		config.addNode(nodeB);
		pool = new ZuliaPool(config);

		var shardMapping = IndexShardMapping.newBuilder()
				.setIndexName("myIndex")
				.setNumberOfShards(2)
				.addShardMapping(ShardMapping.newBuilder().setShardNumber(0).setPrimaryNode(nodeA))
				.addShardMapping(ShardMapping.newBuilder().setShardNumber(1).setPrimaryNode(nodeB))
				.build();
		pool.updateIndexMappings(List.of(shardMapping), List.of());

		// Same uniqueId should always route to the same node
		var cmd = new StubShardCommand("myIndex", "doc-42");
		var result1 = pool.execute(cmd);
		var result2 = pool.execute(cmd);
		assertEquals(result1.getNodeAddress(), result2.getNodeAddress());
	}

	@Test
	void singleIndexRoutableCommandRoutesToIndexNode() throws Exception {
		var nodeA = makeNode("hostA", 32191);

		var config = new ZuliaPoolConfig();
		config.setNodeUpdateEnabled(false);
		config.setRoutingEnabled(true);
		config.setConnectionListener(null);
		config.addNode(nodeA);
		pool = new ZuliaPool(config);

		var shardMapping = IndexShardMapping.newBuilder()
				.setIndexName("myIndex")
				.setNumberOfShards(1)
				.addShardMapping(ShardMapping.newBuilder().setShardNumber(0).setPrimaryNode(nodeA))
				.build();
		pool.updateIndexMappings(List.of(shardMapping), List.of());

		var result = pool.execute(new StubSingleIndexCommand("myIndex"));
		assertEquals("hostA", result.getNodeAddress());
	}

	@Test
	void multiIndexRoutableCommandRoutesAcrossIndexNodes() throws Exception {
		var nodeA = makeNode("hostA", 32191);
		var nodeB = makeNode("hostB", 32191);
		var nodeC = makeNode("hostC", 32191);

		var config = new ZuliaPoolConfig();
		config.setNodeUpdateEnabled(false);
		config.setRoutingEnabled(true);
		config.setConnectionListener(null);
		config.addNode(nodeA);
		config.addNode(nodeB);
		config.addNode(nodeC);
		pool = new ZuliaPool(config);

		// idx1 is on nodeA, idx2 is on nodeB, nodeC hosts neither
		var mapping1 = IndexShardMapping.newBuilder()
				.setIndexName("idx1")
				.setNumberOfShards(1)
				.addShardMapping(ShardMapping.newBuilder().setShardNumber(0).setPrimaryNode(nodeA))
				.build();
		var mapping2 = IndexShardMapping.newBuilder()
				.setIndexName("idx2")
				.setNumberOfShards(1)
				.addShardMapping(ShardMapping.newBuilder().setShardNumber(0).setPrimaryNode(nodeB))
				.build();
		pool.updateIndexMappings(List.of(mapping1, mapping2), List.of());

		// Querying both indexes should only route to nodes hosting those indexes (never nodeC)
		var hosts = new HashSet<String>();
		for (int i = 0; i < 100; i++) {
			var result = pool.execute(new StubMultiIndexCommand(List.of("idx1", "idx2")));
			hosts.add(result.getNodeAddress());
		}
		assertEquals(Set.of("hostA", "hostB"), hosts);
	}

	@Test
	void routingFallsBackToRandomWhenIndexUnknown() throws Exception {
		var nodeA = makeNode("hostA", 32191);

		var config = new ZuliaPoolConfig();
		config.setNodeUpdateEnabled(false);
		config.setRoutingEnabled(true);
		config.setConnectionListener(null);
		config.addNode(nodeA);
		pool = new ZuliaPool(config);

		// Routing enabled, but no index mappings loaded should fall back to random
		var result = pool.execute(new StubSingleIndexCommand("unknownIndex"));
		assertEquals("hostA", result.getNodeAddress());
	}

	// Retry logic

	@Test
	void failingCommandWithZeroRetriesThrowsAfterOneAttempt() {
		var node = makeNode("localhost", 32191);
		var config = configWithNodes(node);
		config.setDefaultRetries(0);
		pool = new ZuliaPool(config);

		var counter = new AtomicInteger(0);
		var cmd = new GrpcCommand<StubResult>() {
			@Override
			public StubResult execute(ZuliaConnection zuliaConnection) {
				counter.incrementAndGet();
				throw new RuntimeException("fail");
			}
		};

		assertThrows(RuntimeException.class, () -> pool.execute(cmd));
		assertEquals(1, counter.get(), "With 0 retries, should try exactly once");
	}

	@Test
	void retriesExhaustedThenThrows() {
		var node = makeNode("localhost", 32191);
		var config = configWithNodes(node);
		config.setDefaultRetries(2);
		pool = new ZuliaPool(config);

		var counter = new AtomicInteger(0);
		var cmd = new GrpcCommand<StubResult>() {
			@Override
			public StubResult execute(ZuliaConnection zuliaConnection) {
				counter.incrementAndGet();
				throw new RuntimeException("fail");
			}
		};

		assertThrows(RuntimeException.class, () -> pool.execute(cmd));
		// Initial try + 2 retries = 3 total attempts
		assertEquals(3, counter.get());
	}

	@Test
	void retrySucceedsOnSecondAttempt() throws Exception {
		var node = makeNode("localhost", 32191);
		var config = configWithNodes(node);
		config.setDefaultRetries(1);
		pool = new ZuliaPool(config);

		var counter = new AtomicInteger(0);
		var cmd = new GrpcCommand<StubResult>() {
			@Override
			public StubResult execute(ZuliaConnection zuliaConnection) {
				if (counter.incrementAndGet() == 1) {
					throw new RuntimeException("transient failure");
				}
				return new StubResult("localhost");
			}
		};

		var result = pool.execute(cmd);
		assertEquals("localhost", result.getNodeAddress());
		assertEquals(2, counter.get());
	}

	// Null connectionListener safety 

	@Test
	void nullConnectionListenerDoesNotNPEOnError() {
		var node = makeNode("localhost", 32191);
		var config = configWithNodes(node);
		config.setConnectionListener(null);
		config.setDefaultRetries(0);
		pool = new ZuliaPool(config);

		assertThrows(RuntimeException.class, () -> pool.execute(new StubGrpcCommand(new RuntimeException("fail"))));
	}

	@Test
	void nullConnectionListenerDoesNotNPEOnRetry() {
		var node = makeNode("localhost", 32191);
		var config = configWithNodes(node);
		config.setConnectionListener(null);
		config.setDefaultRetries(1);
		pool = new ZuliaPool(config);

		assertThrows(RuntimeException.class, () -> pool.execute(new StubGrpcCommand(new RuntimeException("fail"))));
	}

	// Connection listener callbacks

	@Test
	void listenerReceivesExceptionOnFinalFailure() {
		var node = makeNode("localhost", 32191);

		var capturedCommand = new AtomicReference<BaseCommand<?>>();
		var capturedEx = new AtomicReference<Exception>();
		var listener = new NoOpConnectionListener() {
			@Override
			public <R extends Result> void exception(Node selectedNode, BaseCommand<R> command, Exception exception) {
				capturedCommand.set(command);
				capturedEx.set(exception);
			}
		};

		var config = configWithNodes(node);
		config.setConnectionListener(listener);
		config.setDefaultRetries(0);
		pool = new ZuliaPool(config);

		var cmd = new StubGrpcCommand(new RuntimeException("test error"));
		assertThrows(RuntimeException.class, () -> pool.execute(cmd));

		assertSame(cmd, capturedCommand.get());
		assertEquals("test error", capturedEx.get().getMessage());
	}

	@Test
	void listenerReceivesRetryCallbacks() {
		var node = makeNode("localhost", 32191);
		var retryCount = new AtomicInteger(0);
		var listener = new NoOpConnectionListener() {
			@Override
			public <R extends Result> void exceptionWithRetry(Node selectedNode, BaseCommand<R> command, Exception e, int tries) {
				retryCount.set(tries);
			}
		};

		var config = configWithNodes(node);
		config.setConnectionListener(listener);
		config.setDefaultRetries(2);
		pool = new ZuliaPool(config);

		assertThrows(RuntimeException.class, () -> pool.execute(new StubGrpcCommand(new RuntimeException("fail"))));
		assertEquals(2, retryCount.get());
	}

	// Node updates 

	@Test
	void updateNodesRemovesStaleFallbackNodes() throws Exception {
		var nodeA = makeNode("hostA", 32191);
		var nodeB = makeNode("hostB", 32191);

		var config = configWithNodes(nodeA, nodeB);
		pool = new ZuliaPool(config);

		// Remove nodeA, keep only nodeB
		pool.updateNodes(List.of(nodeB));

		// All executions should now go to nodeB
		for (int i = 0; i < 20; i++) {
			var result = pool.execute(new StubGrpcCommand());
			assertEquals("hostB", result.getNodeAddress());
		}
	}

	@Test
	void updateNodesAddsNewNodes() throws Exception {
		var nodeA = makeNode("hostA", 32191);
		var config = configWithNodes(nodeA);
		pool = new ZuliaPool(config);

		// Add nodeB
		var nodeB = makeNode("hostB", 32191);
		pool.updateNodes(List.of(nodeA, nodeB));

		var hosts = new HashSet<String>();
		for (int i = 0; i < 100; i++) {
			var result = pool.execute(new StubGrpcCommand());
			hosts.add(result.getNodeAddress());
		}
		assertEquals(2, hosts.size());
	}

	// Close 

	@Test
	void closeIsIdempotent() {
		var node = makeNode("localhost", 32191);
		pool = new ZuliaPool(configWithNodes(node));
		pool.close();
		pool.close(); // should not throw
	}
}
