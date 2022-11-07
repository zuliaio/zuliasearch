package io.zulia.server.index;

import io.zulia.ZuliaConstants;
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
import io.zulia.util.ZuliaUtil;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.bson.Document;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

public class ZuliaShard {

	private final static Logger LOG = Logger.getLogger(ZuliaShard.class.getSimpleName());

	private final int shardNumber;

	private final ShardReaderManager shardReaderManager;
	private final ShardWriteManager shardWriteManager;
	private final String indexName;

	private final boolean primary;

	private String trackingId;
	private HashSet<String> trackedIds;

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

		shardReaderManager.maybeRefreshBlocking();
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
			throw new IllegalStateException("Cannot force commit from replica:  index <" + indexName + "> shard <" + shardNumber + ">");
		}

		shardWriteManager.commit();
		shardReaderManager.maybeRefresh();

	}

	public void tryIdleCommit() throws IOException {

		if (shardWriteManager.needsIdleCommit()) {
			forceCommit();
		}
	}

	public void tryWarmSearches(ZuliaIndex zuliaIndex, boolean primary) {

		EnumSet<MasterSlaveSettings> usesPrimary = EnumSet.of(MasterSlaveSettings.MASTER_ONLY, MasterSlaveSettings.MASTER_IF_AVAILABLE);
		EnumSet<MasterSlaveSettings> usesReplica = EnumSet.of(MasterSlaveSettings.SLAVE_ONLY, MasterSlaveSettings.MASTER_IF_AVAILABLE);

		if (shardWriteManager.needsSearchWarming()) {

			List<ZuliaServiceOuterClass.QueryRequest> warmingSearches = shardWriteManager.getIndexConfig().getWarmingSearches();

			for (ZuliaServiceOuterClass.QueryRequest warmingSearch : warmingSearches) {
				MasterSlaveSettings primaryReplicaSettings = warmingSearch.getMasterSlaveSettings();
				boolean shardNeedsWarmForSearch = (primary ? usesPrimary : usesReplica).contains(primaryReplicaSettings);

				if (shardNeedsWarmForSearch) {
					try {
						LOG.info("Warming search with label <" + warmingSearch.getSearchLabel() + ">");
						Query query = zuliaIndex.getQuery(warmingSearch);
						ShardQuery shardQuery = zuliaIndex.getShardQuery(query, warmingSearch);
						queryShard(shardQuery);
					}
					catch (Exception e) {
						LOG.severe("Failed to warm search with label <" + warmingSearch.getSearchLabel() + ">: " + e.getMessage());
					}
				}

				if (!shardWriteManager.needsSearchWarming()) {
					break;
				}
			}

			shardWriteManager.searchesWarmed();
		}
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
					throw new RuntimeException("Reindex interrupted by another reindex");
				}

				try {
					IndexableField f = d.getField(ZuliaConstants.TIMESTAMP_FIELD);
					long timestamp = f.numericValue().longValue();

					String uniqueId = d.get(ZuliaConstants.ID_FIELD);

					Document mongoDocument = null;
					Document metadata = null;

					BytesRef metaRef = d.getBinaryValue(ZuliaConstants.STORED_META_FIELD);
					if (metaRef != null) {
						metadata = ZuliaUtil.byteArrayToMongoDocument(metaRef.bytes);
					}

					BytesRef docRef = d.getBinaryValue(ZuliaConstants.STORED_DOC_FIELD);
					if (docRef != null) {
						mongoDocument = ZuliaUtil.byteArrayToMongoDocument(docRef.bytes);
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
			LOG.info("Reindexed <" + count.get() + "> documents for shard <" + shardNumber + "> for index <" + indexName + ">");
			forceCommit();
		}
		finally {
			shardReaderManager.decRef(shardReader);
		}
	}

	public void close() throws IOException {
		shardWriteManager.close();
	}

	public void index(String uniqueId, long timestamp, org.bson.Document mongoDocument, org.bson.Document metadata) throws Exception {
		if (!primary) {
			throw new IllegalStateException("Cannot index document <" + uniqueId + "> from replica:  index <" + indexName + "> shard <" + shardNumber + ">");
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
			throw new IllegalStateException("Cannot delete document <" + uniqueId + "> from replica:  index <" + indexName + "> shard <" + shardNumber + ">");
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
			throw new IllegalStateException("Cannot optimize replica:  index <" + indexName + "> shard <" + shardNumber + ">");
		}

		shardWriteManager.forceMerge(maxNumberSegments);
		forceCommit();
	}

	public void clear() throws IOException {
		if (!primary) {
			throw new IllegalStateException("Cannot clear replica:  index <" + indexName + "> shard <" + shardNumber + ">");
		}

		shardWriteManager.deleteAll();
		forceCommit();
	}

	public GetFieldNamesResponse getFieldNames() throws IOException {
		shardReaderManager.maybeRefreshBlocking();
		ShardReader shardReader = shardReaderManager.acquire();

		try {
			return shardReader.getFields();
		}
		finally {
			shardReaderManager.decRef(shardReader);
		}
	}

	public GetTermsResponse getTerms(GetTermsRequest request) throws IOException {

		shardReaderManager.maybeRefreshBlocking();
		ShardReader shardReader = shardReaderManager.acquire();

		try {
			ShardTermsHandler shardTermsHandler = shardReader.getShardTermsHandler();
			return shardTermsHandler.handleShardTerms(request);

		}
		finally {
			shardReaderManager.decRef(shardReader);
		}

	}

	public ShardCountResponse getNumberOfDocs() throws IOException {

		shardReaderManager.maybeRefreshBlocking();
		ShardReader shardReader = shardReaderManager.acquire();

		try {
			int count = shardReader.numDocs();
			return ShardCountResponse.newBuilder().setNumberOfDocs(count).setShardNumber(shardNumber).build();
		}
		finally {
			shardReaderManager.decRef(shardReader);
		}

	}

	public ZuliaBase.ResultDocument getSourceDocument(String uniqueId, FetchType resultFetchType, List<String> fieldsToReturn, List<String> fieldsToMask)
			throws Exception {
		shardReaderManager.maybeRefreshBlocking();
		ShardReader shardReader = shardReaderManager.acquire();

		try {
			return shardReader.getSourceDocument(uniqueId, resultFetchType, fieldsToReturn, fieldsToMask);
		}
		finally {
			shardReaderManager.decRef(shardReader);
		}
	}

	public ZuliaBase.ShardCacheStats getShardCacheStats() throws IOException {
		shardReaderManager.maybeRefreshBlocking();
		ShardReader shardReader = shardReaderManager.acquire();

		try {
			return shardReader.getShardCacheStats();
		}
		finally {
			shardReaderManager.decRef(shardReader);
		}
	}

}
