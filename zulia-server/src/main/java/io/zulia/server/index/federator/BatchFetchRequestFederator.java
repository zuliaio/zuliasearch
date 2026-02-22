package io.zulia.server.index.federator;

import io.zulia.message.ZuliaBase;
import io.zulia.message.ZuliaBase.Node;
import io.zulia.message.ZuliaQuery.FetchType;
import io.zulia.message.ZuliaServiceOuterClass.BatchFetchGroup;
import io.zulia.message.ZuliaServiceOuterClass.BatchFetchRequest;
import io.zulia.message.ZuliaServiceOuterClass.FetchRequest;
import io.zulia.message.ZuliaServiceOuterClass.FetchResponse;
import io.zulia.message.ZuliaServiceOuterClass.InternalBatchFetchRequest;
import io.zulia.message.ZuliaServiceOuterClass.InternalShardBatchFetchRequest;
import io.zulia.server.connection.client.InternalClient;
import io.zulia.server.index.MasterSlaveSelector;
import io.zulia.server.index.NodeRequestBase;
import io.zulia.server.index.ZuliaIndex;
import io.zulia.util.ShardUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class BatchFetchRequestFederator extends NodeRequestBase<InternalBatchFetchRequest, List<FetchResponse>> {

	private final ExecutorService pool;
	private final InternalClient internalClient;
	private final Map<String, ZuliaIndex> indexCache;

	public BatchFetchRequestFederator(Node thisNode, Collection<Node> otherNodesActive, ExecutorService pool, InternalClient internalClient,
			Map<String, ZuliaIndex> indexCache) {
		super(thisNode, otherNodesActive);
		this.pool = pool;
		this.internalClient = internalClient;
		this.indexCache = indexCache;
	}

	@Override
	protected List<FetchResponse> processInternal(Node node, InternalBatchFetchRequest request) throws Exception {
		List<FetchResponse> allResponses = new ArrayList<>();
		for (InternalShardBatchFetchRequest shardRequest : request.getShardBatchFetchRequestList()) {
			ZuliaIndex index = indexCache.get(shardRequest.getIndexName());
			allResponses.addAll(index.internalShardBatchFetch(shardRequest));
		}
		return allResponses;
	}

	@Override
	protected List<FetchResponse> processExternal(Node node, InternalBatchFetchRequest request) throws Exception {
		return internalClient.executeBatchFetch(node, request).getFetchResponseList();
	}

	public List<FetchResponse> send(BatchFetchRequest request) throws Exception {

		List<FetchRequest> fetchRequests = request.getFetchRequestList();
		List<BatchFetchGroup> batchFetchGroups = request.getBatchFetchGroupList();
		if (fetchRequests.isEmpty() && batchFetchGroups.isEmpty()) {
			return List.of();
		}

		ZuliaBase.MasterSlaveSettings masterSlaveSettings = !fetchRequests.isEmpty() ? fetchRequests.getFirst().getMasterSlaveSettings()
				: ZuliaBase.MasterSlaveSettings.MASTER_ONLY;

		List<Node> nodesAvailable = new ArrayList<>();
		nodesAvailable.add(thisNode);
		nodesAvailable.addAll(otherNodesActive);

		// Group into shard-level batches keyed by (node, indexName, shardNumber, fetchProfile)
		record ShardKey(Node node, String indexName, int shardNumber, FetchType resultFetchType, List<String> documentFields,
				List<String> documentMaskedFields, boolean realtime, FetchType associatedFetchType, String filename) {
		}
		Map<ShardKey, List<String>> shardGroups = new LinkedHashMap<>();

		for (FetchRequest fetchRequest : fetchRequests) {
			String indexName = fetchRequest.getIndexName();
			ZuliaIndex index = indexCache.get(indexName);
			MasterSlaveSelector selector = new MasterSlaveSelector(masterSlaveSettings, nodesAvailable, index.getIndexShardMapping());
			Node targetNode = selector.getNodeForUniqueId(fetchRequest.getUniqueId());
			int shardNumber = ShardUtil.findShardForUniqueId(fetchRequest.getUniqueId(), index.getNumberOfShards());
			ShardKey key = new ShardKey(targetNode, indexName, shardNumber, fetchRequest.getResultFetchType(), fetchRequest.getDocumentFieldsList(),
					fetchRequest.getDocumentMaskedFieldsList(), fetchRequest.getRealtime(), fetchRequest.getAssociatedFetchType(),
					fetchRequest.getFilename());
			shardGroups.computeIfAbsent(key, k -> new ArrayList<>()).add(fetchRequest.getUniqueId());
		}

		for (BatchFetchGroup group : batchFetchGroups) {
			String indexName = group.getIndexName();
			ZuliaIndex index = indexCache.get(indexName);
			MasterSlaveSelector selector = new MasterSlaveSelector(masterSlaveSettings, nodesAvailable, index.getIndexShardMapping());
			for (String uniqueId : group.getUniqueIdList()) {
				Node targetNode = selector.getNodeForUniqueId(uniqueId);
				int shardNumber = ShardUtil.findShardForUniqueId(uniqueId, index.getNumberOfShards());
				ShardKey key = new ShardKey(targetNode, indexName, shardNumber, group.getResultFetchType(), group.getDocumentFieldsList(),
						group.getDocumentMaskedFieldsList(), group.getRealtime(), group.getAssociatedFetchType(), group.getFilename());
				shardGroups.computeIfAbsent(key, k -> new ArrayList<>()).add(uniqueId);
			}
		}

		// Group shard batches by node into InternalBatchFetchRequests
		Map<Node, InternalBatchFetchRequest.Builder> nodeRequests = new LinkedHashMap<>();
		for (Map.Entry<ShardKey, List<String>> entry : shardGroups.entrySet()) {
			ShardKey key = entry.getKey();
			InternalShardBatchFetchRequest shardRequest = InternalShardBatchFetchRequest.newBuilder().setIndexName(key.indexName())
					.setShardNumber(key.shardNumber()).addAllUniqueId(entry.getValue()).setResultFetchType(key.resultFetchType())
					.addAllDocumentFields(key.documentFields()).addAllDocumentMaskedFields(key.documentMaskedFields()).setRealtime(key.realtime())
					.setAssociatedFetchType(key.associatedFetchType()).setFilename(key.filename()).build();
			nodeRequests.computeIfAbsent(key.node(), k -> InternalBatchFetchRequest.newBuilder()).addShardBatchFetchRequest(shardRequest);
		}

		// Submit work per node in parallel
		List<Future<List<FetchResponse>>> futures = new ArrayList<>(nodeRequests.size());

		for (Map.Entry<Node, InternalBatchFetchRequest.Builder> entry : nodeRequests.entrySet()) {
			Node node = entry.getKey();
			InternalBatchFetchRequest nodeRequest = entry.getValue().build();

			if (nodeIsLocal(node)) {
				futures.add(pool.submit(() -> processInternal(node, nodeRequest)));
			}
			else {
				futures.add(pool.submit(() -> processExternal(node, nodeRequest)));
			}
		}

		List<FetchResponse> allResponses = new ArrayList<>();
		for (Future<List<FetchResponse>> future : futures) {
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
