package io.zulia.server.index;

import io.zulia.ZuliaConstants;
import io.zulia.message.ZuliaBase;
import io.zulia.message.ZuliaBase.ShardCountResponse;
import io.zulia.message.ZuliaBase.Similarity;
import io.zulia.message.ZuliaQuery;
import io.zulia.message.ZuliaQuery.FacetRequest;
import io.zulia.message.ZuliaQuery.FetchType;
import io.zulia.message.ZuliaQuery.HighlightRequest;
import io.zulia.message.ZuliaQuery.ShardQueryResponse;
import io.zulia.message.ZuliaQuery.SortRequest;
import io.zulia.message.ZuliaServiceOuterClass.GetFieldNamesResponse;
import io.zulia.message.ZuliaServiceOuterClass.GetTermsRequest;
import io.zulia.message.ZuliaServiceOuterClass.GetTermsResponse;
import io.zulia.server.search.QueryCacheKey;
import io.zulia.util.ZuliaUtil;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.bson.Document;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class ZuliaShard {

	private final static Logger LOG = Logger.getLogger(ZuliaShard.class.getSimpleName());

	private final int shardNumber;

	private final ShardReaderManager shardReaderManager;
	private final ShardWriteManager shardWriteManager;
	private final String indexName;

	private final boolean primary;

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

	public ShardQueryResponse queryShard(Query query, Map<String, Similarity> similarityOverrideMap, int amount, FieldDoc after, FacetRequest facetRequest,
			SortRequest sortRequest, QueryCacheKey queryCacheKey, FetchType resultFetchType, List<String> fieldsToReturn, List<String> fieldsToMask,
			List<HighlightRequest> highlightList, List<ZuliaQuery.AnalysisRequest> analysisRequestList, boolean debug) throws Exception {

		shardReaderManager.maybeRefreshBlocking();
		ShardReader shardReader = shardReaderManager.acquire();

		try {
			return shardReader
					.queryShard(query, similarityOverrideMap, amount, after, facetRequest, sortRequest, queryCacheKey, resultFetchType, fieldsToReturn,
							fieldsToMask, highlightList, analysisRequestList, debug);
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

	public void reindex() throws IOException {
		shardReaderManager.maybeRefreshBlocking();
		ShardReader shardReader = shardReaderManager.acquire();

		try {
			shardReader.streamAllDocs(d -> {
				try {
					IndexableField f = d.getField(ZuliaConstants.TIMESTAMP_FIELD);
					long timestamp = f.numericValue().longValue();

					ZuliaQuery.ScoredResult.Builder srBuilder = ZuliaQuery.ScoredResult.newBuilder();
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

					shardWriteManager.indexDocument(uniqueId, timestamp, mongoDocument, metadata);
				}
				catch (Exception e) {
					throw new RuntimeException(e);
				}
			});
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

		shardWriteManager.indexDocument(uniqueId, timestamp, mongoDocument, metadata);
		if (shardWriteManager.markedChangedCheckIfCommitNeeded()) {
			forceCommit();
		}

	}

	public void deleteDocument(String uniqueId) throws Exception {
		if (!primary) {
			throw new IllegalStateException("Cannot delete document <" + uniqueId + "> from replica:  index <" + indexName + "> shard <" + shardNumber + ">");
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
}
