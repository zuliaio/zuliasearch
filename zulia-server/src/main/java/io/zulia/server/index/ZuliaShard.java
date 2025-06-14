package io.zulia.server.index;

import io.zulia.message.ZuliaBase;
import io.zulia.message.ZuliaBase.MasterSlaveSettings;
import io.zulia.message.ZuliaBase.ShardCountResponse;
import io.zulia.message.ZuliaQuery.FetchType;
import io.zulia.message.ZuliaQuery.ShardQueryResponse;
import io.zulia.message.ZuliaServiceOuterClass;
import io.zulia.message.ZuliaServiceOuterClass.GetFieldNamesResponse;
import io.zulia.message.ZuliaServiceOuterClass.GetTermsRequest;
import io.zulia.message.ZuliaServiceOuterClass.GetTermsResponse;
import io.zulia.server.search.ShardQuery;
import io.zulia.server.util.BytesRefUtil;
import org.apache.lucene.search.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class ZuliaShard {

	private final static Logger LOG = LoggerFactory.getLogger(ZuliaShard.class);

	private final int shardNumber;

	private final ShardReaderManager shardReaderManager;
	private final ShardWriteManager shardWriteManager;
	private final String indexName;

	private final boolean primary;

	private String trackingId;
	private HashSet<String> trackedIds;

	private boolean unloaded;

	public ZuliaShard(ShardWriteManager shardWriteManager, boolean primary) throws Exception {

		this.primary = primary;
		this.shardWriteManager = shardWriteManager;
		this.shardNumber = shardWriteManager.getShardNumber();
		this.indexName = shardWriteManager.getIndexConfig().getIndexName();
		this.shardReaderManager = new ShardReaderManager(shardWriteManager.createShardReader());

	}

	public boolean isPrimary() {
		return primary;
	}

	public void updateIndexSettings() {
		shardWriteManager.updateIndexSettings();
	}

	public int getShardNumber() {
		return shardWriteManager.getShardNumber();
	}

	public ShardQueryResponse queryShard(ShardQuery shardQuery) throws Exception {

		if (shardQuery.isRealtime()) {
			shardReaderManager.maybeRefreshBlocking();
		}
		ShardReader shardReader = shardReaderManager.acquire();

		try {
			return shardReader.queryShard(shardQuery);
		}
		finally {
			shardReaderManager.decRef(shardReader);
		}
	}

	public void forceCommit() throws IOException {
		if (!primary) {
			throw new IllegalStateException("Cannot force commit from replica for index " + indexName + ":s" + shardNumber);
		}

		shardWriteManager.commit();
		shardReaderManager.maybeRefreshBlocking();
	}

	public void tryIdleCommit() throws IOException {

		if (shardWriteManager.needsIdleCommit()) {
			LOG.info("Index {}:s{} is idle, triggering commit", indexName, shardNumber);
			forceCommit();
		}
	}

	public void tryWarmSearches(ZuliaIndex zuliaIndex, boolean primary) {

		long lastestShardTime = shardReaderManager.getLatestShardTime();
		WarmInfo warmInfo = shardWriteManager.needsSearchWarming(lastestShardTime);
		if (warmInfo.needsWarming()) {

			try {
				shardReaderManager.maybeRefreshBlocking();
			}
			catch (Exception e) {
				LOG.error("Failed to refresh shard reader: ", e);
				throw new RuntimeException(e);
			}

			List<ZuliaServiceOuterClass.QueryRequest> warmingSearches = shardWriteManager.getIndexConfig().getWarmingSearches();
			if (!warmingSearches.isEmpty()) {
				warmSearches(zuliaIndex, primary, warmingSearches, warmInfo, lastestShardTime);
			}
		}
	}

	private void warmSearches(ZuliaIndex zuliaIndex, boolean primary, List<ZuliaServiceOuterClass.QueryRequest> warmingSearches, WarmInfo warmInfo,
			long lastestShardTime) {
		LOG.info("Started warming searching for index {}:s{}", indexName, shardNumber);
		EnumSet<MasterSlaveSettings> usesPrimary = EnumSet.of(MasterSlaveSettings.MASTER_ONLY, MasterSlaveSettings.MASTER_IF_AVAILABLE);
		EnumSet<MasterSlaveSettings> usesReplica = EnumSet.of(MasterSlaveSettings.SLAVE_ONLY, MasterSlaveSettings.MASTER_IF_AVAILABLE);
		for (ZuliaServiceOuterClass.QueryRequest warmingSearch : warmingSearches) {
			MasterSlaveSettings primaryReplicaSettings = warmingSearch.getMasterSlaveSettings();
			boolean shardNeedsWarmForSearch = (primary ? usesPrimary : usesReplica).contains(primaryReplicaSettings);

			if (unloaded) {
				return;
			}

			// has the index changed since we made the decision to start warming ?
			if (!Objects.equals(warmInfo.lastChanged(), shardWriteManager.getLastChanged())) {
				LOG.info("Index {}:s{} changed: canceling warming", indexName, shardNumber);
				return;
			}
			if (!Objects.equals(warmInfo.lastCommit(), shardWriteManager.getLastCommit())) {
				LOG.info("Index {}:s{} commited: canceling warming", indexName, shardNumber);
				return;
			}
			if (lastestShardTime != shardReaderManager.getLatestShardTime()) {
				LOG.info("Index {}:s{} reloaded: canceling warming", indexName, shardNumber);
				return;
			}

			if (shardNeedsWarmForSearch) {
				try {
					LOG.info("Warming search for index {}:s{} with label {}", indexName, shardNumber, warmingSearch.getSearchLabel());
					Query query = zuliaIndex.getQuery(warmingSearch);
					ShardQuery shardQuery = zuliaIndex.getShardQuery(query, warmingSearch);
					queryShard(shardQuery);
				}
				catch (Exception e) {
					LOG.error("Warming search for index {}:s{} with label {}: {}", indexName, shardNumber, warmingSearch.getSearchLabel(), e.getMessage());
				}
			}

		}

		// has the index changed since we made the decision to start warming ?
		if (!Objects.equals(warmInfo.lastChanged(), shardWriteManager.getLastChanged())) {
			LOG.info("Index {}:s{} changed: canceling warming", indexName, shardNumber);
			return;
		}
		if (!Objects.equals(warmInfo.lastCommit(), shardWriteManager.getLastCommit())) {
			LOG.info("Index {}:s{} commited: canceling warming", indexName, shardNumber);
			return;
		}

		long warmTime = System.currentTimeMillis();
		if (lastestShardTime != shardReaderManager.getLatestShardTime()) {
			LOG.info("Index {}:s{} reloaded: canceling warming", indexName, shardNumber);
			return;
		}

		shardWriteManager.searchesWarmed(warmTime);
		LOG.info("Finished warming searching for index {}:s{}", indexName, shardNumber);
	}

	public void reindex() throws IOException {

		final String myTrackingId = UUID.randomUUID().toString();
		synchronized (this) {
			trackingId = myTrackingId;
			trackedIds = new HashSet<>();
		}

		shardReaderManager.maybeRefreshBlocking();
		ShardReader shardReader = shardReaderManager.acquire();

		try {
			AtomicInteger count = new AtomicInteger();
			shardReader.streamAllDocs(d -> {
				if (!myTrackingId.equals(trackingId)) {
					throw new RuntimeException("Reindex for " + indexName + ":s" + shardNumber + " interrupted by another reindex");
				}

				try {

					byte[] idInfoBytes = BytesRefUtil.getByteArray(d.idInfo());
					ZuliaBase.IdInfo idInfo = ZuliaBase.IdInfo.parseFrom(idInfoBytes);

					long timestamp = idInfo.getTimestamp();

					String uniqueId = idInfo.getId();

					DocumentContainer metadata;
					DocumentContainer mongoDocument;
					if (idInfo.getCompressedDoc()) {
						metadata = new DocumentContainer(d.meta() != null ? Snappy.uncompress(BytesRefUtil.getByteArray(d.meta())) : null);
						mongoDocument = new DocumentContainer(d.fullDoc() != null ? Snappy.uncompress(BytesRefUtil.getByteArray(d.fullDoc())) : null);
					}
					else {
						metadata = new DocumentContainer(d.meta());
						mongoDocument = new DocumentContainer(d.fullDoc());
					}

					if (!trackedIds.contains(uniqueId)) {
						shardWriteManager.indexDocument(uniqueId, timestamp, mongoDocument, metadata);
					}
					count.getAndIncrement();
				}
				catch (Exception e) {
					throw new RuntimeException(e);
				}
			});
			synchronized (this) {
				if (myTrackingId.equals(trackingId)) {
					trackingId = null;
					trackedIds = new HashSet<>();
				}
			}
			LOG.info("Re-indexed {} documents for index {}:s{}", count.get(), indexName, shardNumber);
			forceCommit();
		}
		finally {
			shardReaderManager.decRef(shardReader);
		}
	}

	public void close() throws IOException {
		unloaded = true;
		shardWriteManager.close();
	}

	public void index(String uniqueId, long timestamp, DocumentContainer mongoDocument, DocumentContainer metadata) throws Exception {
		if (!primary) {
			throw new IllegalStateException("Cannot index document " + uniqueId + " from replica:  index " + indexName + ":s" + shardNumber);
		}

		if (trackingId != null) {
			trackedIds.add(uniqueId);
		}

		shardWriteManager.indexDocument(uniqueId, timestamp, mongoDocument, metadata);
		if (shardWriteManager.markedChangedCheckIfCommitNeeded()) {
			forceCommit();
		}

	}

	public void deleteDocument(String uniqueId) throws Exception {
		if (!primary) {
			throw new IllegalStateException("Cannot delete document " + uniqueId + " from replica:  index " + indexName + ":s" + shardNumber);
		}

		if (trackingId != null) {
			trackedIds.add(uniqueId);
		}

		shardWriteManager.deleteDocuments(uniqueId);
		if (shardWriteManager.markedChangedCheckIfCommitNeeded()) {
			forceCommit();
		}

	}

	public void optimize(int maxNumberSegments) throws IOException {
		if (!primary) {
			throw new IllegalStateException("Cannot optimize replica for index " + indexName + ":s" + shardNumber);
		}

		shardWriteManager.forceMerge(maxNumberSegments);
		forceCommit();
	}

	public void clear() throws IOException {
		if (!primary) {
			throw new IllegalStateException("Cannot clear replica for index " + indexName + ":s" + shardNumber);
		}

		shardWriteManager.deleteAll();
		forceCommit();
	}

	public GetFieldNamesResponse getFieldNames(boolean realtime) throws IOException {

		if (realtime) {
			shardReaderManager.maybeRefreshBlocking();
		}
		ShardReader shardReader = shardReaderManager.acquire();

		try {
			return shardReader.getFields();
		}
		finally {
			shardReaderManager.decRef(shardReader);
		}
	}

	public GetTermsResponse getTerms(GetTermsRequest request) throws IOException {

		if (request.getRealtime()) {
			shardReaderManager.maybeRefreshBlocking();
		}
		ShardReader shardReader = shardReaderManager.acquire();

		try {
			ShardTermsHandler shardTermsHandler = shardReader.getShardTermsHandler();
			return shardTermsHandler.handleShardTerms(request);

		}
		finally {
			shardReaderManager.decRef(shardReader);
		}

	}

	public ShardCountResponse getNumberOfDocs(boolean realtime) throws IOException {

		if (realtime) {
			shardReaderManager.maybeRefreshBlocking();
		}
		ShardReader shardReader = shardReaderManager.acquire();

		try {
			int count = shardReader.numDocs();
			return ShardCountResponse.newBuilder().setNumberOfDocs(count).setShardNumber(shardNumber).build();
		}
		finally {
			shardReaderManager.decRef(shardReader);
		}

	}

	public ZuliaBase.ResultDocument getSourceDocument(String uniqueId, FetchType resultFetchType, List<String> fieldsToReturn, List<String> fieldsToMask, boolean realtime)
			throws Exception {
		if (realtime) {
			shardReaderManager.maybeRefreshBlocking();
		}
		ShardReader shardReader = shardReaderManager.acquire();

		try {
			return shardReader.getSourceDocument(uniqueId, resultFetchType, fieldsToReturn, fieldsToMask, realtime);
		}
		finally {
			shardReaderManager.decRef(shardReader);
		}
	}

	public ZuliaBase.ShardCacheStats getShardCacheStats(boolean realtime) throws IOException {

		if (realtime) {
			//shardReaderManager.maybeRefreshBlocking();
		}
		ShardReader shardReader = shardReaderManager.acquire();

		try {
			return shardReader.getShardCacheStats();
		}
		finally {
			shardReaderManager.decRef(shardReader);
		}
	}

}
