package io.zulia.server.index.federator;

import io.zulia.message.ZuliaBase.MasterSlaveSettings;
import io.zulia.message.ZuliaBase.Node;
import io.zulia.message.ZuliaServiceOuterClass.BatchDeleteGroup;
import io.zulia.message.ZuliaServiceOuterClass.BatchDeleteRequest;
import io.zulia.message.ZuliaServiceOuterClass.DeleteRequest;
import io.zulia.message.ZuliaServiceOuterClass.DeleteResponse;
import io.zulia.message.ZuliaServiceOuterClass.InternalBatchDeleteRequest;
import io.zulia.message.ZuliaServiceOuterClass.InternalShardBatchDeleteRequest;
import io.zulia.server.connection.client.InternalClient;
import io.zulia.server.index.MasterSlaveSelector;
import io.zulia.server.index.NodeRequestBase;
import io.zulia.server.index.ZuliaIndex;
import io.zulia.util.ShardUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class BatchDeleteRequestFederator extends NodeRequestBase<InternalBatchDeleteRequest, List<DeleteResponse>> {

	private final ExecutorService pool;
	private final InternalClient internalClient;
	private final Map<String, ZuliaIndex> indexCache;

	public BatchDeleteRequestFederator(Node thisNode, Collection<Node> otherNodesActive, ExecutorService pool, InternalClient internalClient,
			Map<String, ZuliaIndex> indexCache) {
		super(thisNode, otherNodesActive);
		this.pool = pool;
		this.internalClient = internalClient;
		this.indexCache = indexCache;
	}

	@Override
	protected List<DeleteResponse> processInternal(Node node, InternalBatchDeleteRequest request) throws Exception {
		List<InternalShardBatchDeleteRequest> shardRequests = request.getShardBatchDeleteRequestList();
		if (shardRequests.size() == 1) {
			ZuliaIndex index = indexCache.get(shardRequests.getFirst().getIndexName());
			return index.internalShardBatchDelete(shardRequests.getFirst());
		}

		List<Future<List<DeleteResponse>>> futures = new ArrayList<>(shardRequests.size());
		for (InternalShardBatchDeleteRequest shardRequest : shardRequests) {
			futures.add(pool.submit(() -> {
				ZuliaIndex index = indexCache.get(shardRequest.getIndexName());
				return index.internalShardBatchDelete(shardRequest);
			}));
		}

		List<DeleteResponse> allResponses = new ArrayList<>();
		for (Future<List<DeleteResponse>> future : futures) {
			allResponses.addAll(future.get());
		}
		return allResponses;
	}

	@Override
	protected List<DeleteResponse> processExternal(Node node, InternalBatchDeleteRequest request) throws Exception {
		return internalClient.executeBatchDelete(node, request).getDeleteResponseList();
	}

	public List<DeleteResponse> send(BatchDeleteRequest request) throws Exception {

		List<DeleteRequest> deleteRequests = request.getRequestList();
		List<BatchDeleteGroup> batchDeleteGroups = request.getBatchDeleteGroupList();
		if (deleteRequests.isEmpty() && batchDeleteGroups.isEmpty()) {
			return List.of();
		}

		List<Node> nodesAvailable = new ArrayList<>();
		nodesAvailable.add(thisNode);
		nodesAvailable.addAll(otherNodesActive);

		// Group into shard-level batches keyed by (node, indexName, shardNumber, deleteProfile)
		record ShardKey(Node node, String indexName, int shardNumber, boolean deleteDocument, boolean deleteAllAssociated, String filename) {
		}
		Map<ShardKey, List<String>> shardGroups = new LinkedHashMap<>();

		Map<String, MasterSlaveSelector> selectorCache = new HashMap<>();

		for (DeleteRequest deleteRequest : deleteRequests) {
			String indexName = deleteRequest.getIndexName();
			MasterSlaveSelector selector = selectorCache.computeIfAbsent(indexName, name -> {
				ZuliaIndex index = indexCache.get(name);
				return new MasterSlaveSelector(MasterSlaveSettings.MASTER_ONLY, nodesAvailable, index.getIndexShardMapping());
			});
			Node targetNode = selector.getNodeForUniqueId(deleteRequest.getUniqueId());
			int shardNumber = ShardUtil.findShardForUniqueId(deleteRequest.getUniqueId(), indexCache.get(indexName).getNumberOfShards());
			ShardKey key = new ShardKey(targetNode, indexName, shardNumber, deleteRequest.getDeleteDocument(), deleteRequest.getDeleteAllAssociated(),
					deleteRequest.getFilename());
			shardGroups.computeIfAbsent(key, k -> new ArrayList<>()).add(deleteRequest.getUniqueId());
		}

		for (BatchDeleteGroup group : batchDeleteGroups) {
			String indexName = group.getIndexName();
			MasterSlaveSelector selector = selectorCache.computeIfAbsent(indexName, name -> {
				ZuliaIndex index = indexCache.get(name);
				return new MasterSlaveSelector(MasterSlaveSettings.MASTER_ONLY, nodesAvailable, index.getIndexShardMapping());
			});
			for (String uniqueId : group.getUniqueIdList()) {
				Node targetNode = selector.getNodeForUniqueId(uniqueId);
				int shardNumber = ShardUtil.findShardForUniqueId(uniqueId, indexCache.get(indexName).getNumberOfShards());
				ShardKey key = new ShardKey(targetNode, indexName, shardNumber, group.getDeleteDocument(), group.getDeleteAllAssociated(),
						group.getFilename());
				shardGroups.computeIfAbsent(key, k -> new ArrayList<>()).add(uniqueId);
			}
		}

		// Group shard batches by node into InternalBatchDeleteRequests
		Map<Node, InternalBatchDeleteRequest.Builder> nodeRequests = new LinkedHashMap<>();
		for (Map.Entry<ShardKey, List<String>> entry : shardGroups.entrySet()) {
			ShardKey key = entry.getKey();
			InternalShardBatchDeleteRequest shardRequest = InternalShardBatchDeleteRequest.newBuilder().setIndexName(key.indexName())
					.setShardNumber(key.shardNumber()).addAllUniqueId(entry.getValue()).setDeleteDocument(key.deleteDocument())
					.setDeleteAllAssociated(key.deleteAllAssociated()).setFilename(key.filename()).build();
			nodeRequests.computeIfAbsent(key.node(), k -> InternalBatchDeleteRequest.newBuilder()).addShardBatchDeleteRequest(shardRequest);
		}

		// Submit work per node in parallel
		List<Future<List<DeleteResponse>>> futures = new ArrayList<>(nodeRequests.size());

		for (Map.Entry<Node, InternalBatchDeleteRequest.Builder> entry : nodeRequests.entrySet()) {
			Node node = entry.getKey();
			InternalBatchDeleteRequest nodeRequest = entry.getValue().build();

			if (nodeIsLocal(node)) {
				futures.add(pool.submit(() -> processInternal(node, nodeRequest)));
			}
			else {
				futures.add(pool.submit(() -> processExternal(node, nodeRequest)));
			}
		}

		List<DeleteResponse> allResponses = new ArrayList<>();
		for (Future<List<DeleteResponse>> future : futures) {
			try {
				allResponses.addAll(future.get());
			}
			catch (ExecutionException e) {
				Throwable cause = e.getCause();
				if (cause instanceof Exception ex) {
					throw ex;
				}
				throw new Exception(cause);
			}
		}
		return allResponses;
	}
}
