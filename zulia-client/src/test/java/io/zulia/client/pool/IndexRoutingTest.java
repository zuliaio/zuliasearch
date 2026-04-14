package io.zulia.client.pool;

import io.zulia.message.ZuliaBase.Node;
import io.zulia.message.ZuliaIndex.IndexAlias;
import io.zulia.message.ZuliaIndex.IndexShardMapping;
import io.zulia.message.ZuliaIndex.ShardMapping;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class IndexRoutingTest {

	private Node nodeA;
	private Node nodeB;
	private IndexRouting routing;

	@BeforeEach
	void setUp() {
		nodeA = Node.newBuilder().setServerAddress("hostA").setServicePort(32191).setRestPort(32192).build();
		nodeB = Node.newBuilder().setServerAddress("hostB").setServicePort(32191).setRestPort(32192).build();

		var shardMapping = IndexShardMapping.newBuilder()
				.setIndexName("testIndex")
				.setNumberOfShards(2)
				.addShardMapping(ShardMapping.newBuilder().setShardNumber(0).setPrimaryNode(nodeA))
				.addShardMapping(ShardMapping.newBuilder().setShardNumber(1).setPrimaryNode(nodeB))
				.build();

		var alias = IndexAlias.newBuilder().setAliasName("testAlias").setIndexName("testIndex").build();

		routing = new IndexRouting(List.of(shardMapping), List.of(alias));
	}

	@Test
	void getNodeRoutesConsistently() {
		var node1 = routing.getNode("testIndex", "doc1");
		var node2 = routing.getNode("testIndex", "doc1");
		assertNotNull(node1);
		assertSame(node1, node2, "Same uniqueId should always route to the same node");
	}

	@Test
	void getNodeDistributesAcrossShards() {
		Set<String> nodesHit = new HashSet<>();
		for (int i = 0; i < 100; i++) {
			var node = routing.getNode("testIndex", "doc-" + i);
			assertNotNull(node);
			nodesHit.add(node.getServerAddress());
		}
		assertEquals(2, nodesHit.size(), "Documents should be distributed across both nodes");
	}

	@Test
	void getNodeReturnsNullForUnknownIndex() {
		assertNull(routing.getNode("unknownIndex", "doc1"));
	}

	@Test
	void aliasResolvesToIndex() {
		var viaIndex = routing.getNode("testIndex", "doc1");
		var viaAlias = routing.getNode("testAlias", "doc1");
		assertNotNull(viaIndex);
		assertEquals(viaIndex, viaAlias, "Alias should resolve to the same node as the real index");
	}

	@Test
	void getRandomNodeReturnsNodeForKnownIndex() {
		var node = routing.getRandomNode("testIndex");
		assertNotNull(node);
		assertTrue(node.equals(nodeA) || node.equals(nodeB));
	}

	@Test
	void getRandomNodeReturnsNullForUnknownIndex() {
		assertNull(routing.getRandomNode("unknownIndex"));
	}

	@Test
	void getRandomNodeForMultipleIndexes() {
		var shardMapping2 = IndexShardMapping.newBuilder()
				.setIndexName("otherIndex")
				.setNumberOfShards(1)
				.addShardMapping(ShardMapping.newBuilder().setShardNumber(0).setPrimaryNode(nodeB))
				.build();

		var multiRouting = new IndexRouting(
				List.of(
						IndexShardMapping.newBuilder()
								.setIndexName("testIndex")
								.setNumberOfShards(1)
								.addShardMapping(ShardMapping.newBuilder().setShardNumber(0).setPrimaryNode(nodeA))
								.build(),
						shardMapping2
				),
				List.of()
		);

		Set<String> nodesHit = new HashSet<>();
		for (int i = 0; i < 100; i++) {
			var node = multiRouting.getRandomNode(List.of("testIndex", "otherIndex"));
			assertNotNull(node);
			nodesHit.add(node.getServerAddress());
		}
		assertEquals(2, nodesHit.size(), "Should pick from nodes hosting either index");
	}

	@Test
	void getRandomNodeForMultipleIndexesReturnsNullWhenOneUnknown() {
		// When any index in the collection is unknown, routing gives up entirely
		// and returns null, which causes ZuliaPool to fall back to random node selection.
		// The server still handles multi-index resolution correctly.
		assertNull(routing.getRandomNode(List.of("testIndex", "unknownIndex")));
	}

	@Test
	void aliasWorksWithGetRandomNode() {
		var node = routing.getRandomNode("testAlias");
		assertNotNull(node);
		assertTrue(node.equals(nodeA) || node.equals(nodeB));
	}

	@Test
	void singleShardAlwaysRoutesToSameNode() {
		var singleShard = IndexShardMapping.newBuilder()
				.setIndexName("singleIndex")
				.setNumberOfShards(1)
				.addShardMapping(ShardMapping.newBuilder().setShardNumber(0).setPrimaryNode(nodeA))
				.build();

		var singleRouting = new IndexRouting(List.of(singleShard), List.of());

		for (int i = 0; i < 50; i++) {
			assertEquals(nodeA, singleRouting.getNode("singleIndex", "doc-" + i));
		}
	}
}
