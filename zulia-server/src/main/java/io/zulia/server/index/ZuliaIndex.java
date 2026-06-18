package io.zulia.server.index;

import com.google.common.primitives.Floats;
import com.google.protobuf.ProtocolStringList;
import io.zulia.ZuliaFieldConstants;
import io.zulia.message.ZuliaBase;
import io.zulia.message.ZuliaBase.AssociatedDocument;
import io.zulia.message.ZuliaBase.PrimaryReplicaSettings;
import io.zulia.message.ZuliaBase.Node;
import io.zulia.message.ZuliaBase.ResultDocument;
import io.zulia.message.ZuliaBase.ShardCountResponse;
import io.zulia.message.ZuliaBase.Similarity;
import io.zulia.message.ZuliaIndex.FieldConfig;
import io.zulia.message.ZuliaIndex.IndexSettings;
import io.zulia.message.ZuliaIndex.IndexShardMapping;
import io.zulia.message.ZuliaIndex.ShardMapping;
import io.zulia.message.ZuliaQuery;
import io.zulia.message.ZuliaQuery.Facet;
import io.zulia.message.ZuliaQuery.FacetRequest;
import io.zulia.message.ZuliaQuery.FetchType;
import io.zulia.message.ZuliaQuery.FieldSimilarity;
import io.zulia.message.ZuliaQuery.IndexShardResponse;
import io.zulia.message.ZuliaQuery.Query.QueryType;
import io.zulia.message.ZuliaQuery.ShardQueryResponse;
import io.zulia.message.ZuliaQuery.SortRequest;
import io.zulia.message.ZuliaServiceOuterClass;
import io.zulia.message.ZuliaServiceOuterClass.*;
import io.zulia.rest.dto.AssociatedMetadataDTO;
import io.zulia.server.analysis.ZuliaPerFieldAnalyzer;
import io.zulia.server.config.IndexFieldInfo;
import io.zulia.server.config.IndexService;
import io.zulia.server.config.ServerIndexConfig;
import io.zulia.server.config.SortFieldInfo;
import io.zulia.server.config.ZuliaConfig;
import io.zulia.server.connection.client.InternalClient;
import io.zulia.server.exceptions.IndexDoesNotExistException;
import io.zulia.server.exceptions.ShardDoesNotExistException;
import io.zulia.server.index.replication.ReplicaDirectoryApplier;
import io.zulia.server.index.replication.ReplicaFileInfo;
import io.zulia.server.index.replication.ReplicationRateLimiter;
import io.zulia.server.index.replication.ReplicationShardState;
import io.zulia.server.index.replication.ReplicationUtil;
import io.zulia.server.index.replication.SegmentReplicationManager;
import io.zulia.server.field.FieldTypeUtil;
import io.zulia.server.filestorage.DocumentStorage;
import io.zulia.server.search.aggregation.AggregationSettings;
import io.zulia.server.search.GeoDistUtil;
import io.zulia.server.search.MoreLikeThisLazyQuery;
import io.zulia.server.search.QueryCacheKey;
import io.zulia.server.search.ShardQuery;
import io.zulia.server.search.queryparser.SetQueryHelper;
import io.zulia.server.search.queryparser.ZuliaFlexibleQueryParser;
import io.zulia.server.util.DeletingFileVisitor;
import io.zulia.util.ShardUtil;
import org.apache.lucene.expressions.Expression;
import org.apache.lucene.expressions.SimpleBindings;
import org.apache.lucene.expressions.js.JavascriptCompiler;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.function.FunctionScoreQuery;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class ZuliaIndex {

	private final static Logger LOG = LoggerFactory.getLogger(ZuliaIndex.class);
	private final ServerIndexConfig indexConfig;
	private final ConcurrentLinkedDeque<ZuliaFlexibleQueryParser> parsers;
	private final ConcurrentHashMap<Integer, ZuliaShard> primaryShardMap;
	private final ConcurrentHashMap<Integer, ZuliaShard> replicaShardMap;
	private final ExecutorService shardPool;
	private final int numberOfShards;
	private final String indexName;
	private final DocumentStorage documentStorage;
	private final ZuliaConfig zuliaConfig;
	private Thread commitThread;
	private Thread warmThread;
	private Thread replicationWatchdogThread;
	private volatile boolean maintenanceRunning = true;
	private final CountDownLatch maintenanceShutdownLatch = new CountDownLatch(1);

	// How often the commit and warm loops check whether work is due (both checks are gated by timestamps).
	private static final long MAINTENANCE_TICK_INTERVAL_MS = 1_000;
	// Worst-case lag before a stale replica starts catching up; the per-shard generation gate makes each
	// pass cheap when nothing has changed.
	private static final long REPLICATION_WATCHDOG_INTERVAL_MS = 30_000;
	// Best-effort wait for each maintenance thread to stop during unload before proceeding.
	private static final long MAINTENANCE_SHUTDOWN_JOIN_MS = 5_000;
	private final ZuliaPerFieldAnalyzer zuliaPerFieldAnalyzer;
	private final IndexService indexService;
	private volatile IndexShardMapping indexShardMapping;
	private final AggregationSettings aggregationSettings;
	private final SegmentReplicationManager segmentReplicationManager;

	public ZuliaIndex(ZuliaConfig zuliaConfig, ServerIndexConfig indexConfig, DocumentStorage documentStorage, IndexService indexService,
			IndexShardMapping indexShardMapping, InternalClient internalClient, ReplicationRateLimiter replicationRateLimiter) {

		this.zuliaConfig = zuliaConfig;
		this.aggregationSettings = new AggregationSettings(zuliaConfig.getHitsPerConcurrentRequest(), zuliaConfig.getMaxFacetsCachedPerDimension());
		this.indexConfig = indexConfig;
		this.indexName = indexConfig.getIndexName();
		this.numberOfShards = indexConfig.getNumberOfShards();
		this.indexService = indexService;
		this.indexShardMapping = indexShardMapping;
		this.segmentReplicationManager = new SegmentReplicationManager(internalClient, indexShardMapping, indexName, zuliaConfig.getReplicaResponseTimeout(),
				replicationRateLimiter);

		this.documentStorage = documentStorage;

		this.shardPool = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name(indexName + "-shard-", 0).factory());

		this.zuliaPerFieldAnalyzer = new ZuliaPerFieldAnalyzer(indexConfig);

		this.parsers = new ConcurrentLinkedDeque<>();

		this.primaryShardMap = new ConcurrentHashMap<>();
		this.replicaShardMap = new ConcurrentHashMap<>();

	}

	// Start the maintenance loops after the index is fully constructed and shards are loaded, so the virtual
	// threads never observe a partially-constructed ZuliaIndex (avoids publishing `this` from the constructor).
	public void startMaintenance() {
		commitThread = Thread.ofVirtual().name(indexName + "-CommitTimer").start(() -> runMaintenanceLoop(MAINTENANCE_TICK_INTERVAL_MS, () -> {
			if (indexConfig.getIndexSettings().getIdleTimeWithoutCommit() != 0) {
				doCommit(false);
			}
		}));

		warmThread = Thread.ofVirtual().name(indexName + "-WarmTimer").start(() -> runMaintenanceLoop(MAINTENANCE_TICK_INTERVAL_MS, () -> {
			for (ZuliaShard shard : primaryShardMap.values()) {
				shard.tryWarmSearches(this, true);
			}
			for (ZuliaShard shard : replicaShardMap.values()) {
				shard.tryWarmSearches(this, false);
			}
		}));

		replicationWatchdogThread = Thread.ofVirtual().name(indexName + "-ReplicationWatchdog")
				.start(() -> runMaintenanceLoop(REPLICATION_WATCHDOG_INTERVAL_MS, () -> segmentReplicationManager.watchdogTick(primaryShardMap.values())));
	}

	// Runs task every intervalMs until unload(). Sleeps on the shutdown latch (not Thread.sleep) so unload()
	// wakes the loop immediately via countDown() without interrupting a thread that may be mid-commit.
	private void runMaintenanceLoop(long intervalMs, Runnable task) {
		while (maintenanceRunning) {
			try {
				if (maintenanceShutdownLatch.await(intervalMs, TimeUnit.MILLISECONDS)) {
					return; // latch released -> shutting down
				}
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return;
			}
			try {
				task.run();
			}
			catch (Exception e) {
				LOG.warn("Maintenance task {} failed for {}", Thread.currentThread().getName(), indexName, e);
			}
		}
	}

	private void joinMaintenance(Thread thread) {
		if (thread == null) {
			return; // maintenance never started (e.g. load failed before startMaintenance)
		}
		try {
			thread.join(MAINTENANCE_SHUTDOWN_JOIN_MS);
			if (thread.isAlive()) {
				LOG.warn("Maintenance thread {} did not stop within {}ms", thread.getName(), MAINTENANCE_SHUTDOWN_JOIN_MS);
			}
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	public SortFieldInfo getSortFieldType(String fieldName) {
		return indexConfig.getSortFieldInfo(fieldName);
	}

	private void doCommit(boolean force) {

		Collection<ZuliaShard> shards = primaryShardMap.values();
		for (ZuliaShard shard : shards) {
			try {
				if (force) {
					shard.forceCommit();
				}
				else {
					shard.tryIdleCommit();
				}
			}
			catch (Exception e) {
				LOG.error("Failed to flush index {}:s{}", indexName, shard.getShardNumber(), e);
			}
		}

	}

	public void unload(boolean terminate) throws IOException {

		LOG.info("Stopping maintenance threads for {}", indexName);
		maintenanceRunning = false;
		maintenanceShutdownLatch.countDown();
		joinMaintenance(commitThread);
		joinMaintenance(warmThread);
		joinMaintenance(replicationWatchdogThread);

		if (!terminate) {
			LOG.info("Committing {}", indexName);
			doCommit(true);
		}

		// After the final commit has fired its replication hook, await in-flight transfers before closing shards.
		segmentReplicationManager.shutdown();

		LOG.info("Shutting down shard pool for {}", indexName);
		shardPool.shutdownNow();

		for (Integer shardNumber : primaryShardMap.keySet()) {
			LOG.info("Unloading primary shard {}:s{}", indexName, shardNumber);
			unloadShard(shardNumber);
			LOG.info("Unloaded primary shard {}:s{}", indexName, shardNumber);
			if (terminate) {
				deleteShardData(shardNumber);
			}
		}

		LOG.info("Deleting replicas");
		for (Integer shardNumber : replicaShardMap.keySet()) {
			LOG.info("Unloading replica shard {}:s{}", indexName, shardNumber);
			unloadShard(shardNumber);
			LOG.info("Unloaded replica shard {}:s{}", indexName, shardNumber);
			if (terminate) {
				deleteShardData(shardNumber);
			}
		}
		LOG.info("Shut down shard pool for {}", indexName);

	}

	private void deleteShardData(int shardNumber) throws IOException {
		LOG.info("Deleting shard {}:s{} from disk", indexName, shardNumber);
		Files.walkFileTree(getPathForIndex(shardNumber), new DeletingFileVisitor());
		Files.walkFileTree(getPathForFacetsIndex(shardNumber), new DeletingFileVisitor());
		LOG.info("Deleted shard {}:s{} from disk", indexName, shardNumber);
	}

	private void loadShard(int shardNumber, boolean primary) throws Exception {
		if (primary) {
			loadPrimaryShard(shardNumber);
		}
		else {
			loadReplicaShard(shardNumber);
		}
	}

	private void loadPrimaryShard(int shardNumber) throws Exception {
		ShardWriteManager shardWriteManager = new ShardWriteManager(shardNumber, getPathForIndex(shardNumber), getPathForFacetsIndex(shardNumber), indexConfig,
				zuliaPerFieldAnalyzer, aggregationSettings);
		ZuliaShard s = new ZuliaShard(shardWriteManager);
		s.setPostCommitHook(() -> segmentReplicationManager.replicateAfterCommit(s));
		LOG.info("Loaded primary shard {}:s{}", indexName, shardNumber);
		primaryShardMap.put(shardNumber, s);
	}

	private void loadReplicaShard(int shardNumber) throws Exception {
		ShardReadManager shardReadManager = new ShardReadManager(shardNumber, getPathForIndex(shardNumber), getPathForFacetsIndex(shardNumber), indexConfig,
				zuliaPerFieldAnalyzer, aggregationSettings);
		ZuliaShard s = new ZuliaShard(shardReadManager);
		LOG.info("Loaded replica shard {}:s{}", indexName, shardNumber);
		replicaShardMap.put(shardNumber, s);
	}

	private Path getPathForIndex(int shardNumber) {
		return Paths.get(zuliaConfig.getDataPath(), "indexes", indexName + "_" + shardNumber + "_idx");
	}

	private Path getPathForFacetsIndex(int shardNumber) {
		return Paths.get(zuliaConfig.getDataPath(), "indexes", indexName + "_" + shardNumber + "_facets");
	}

	protected void unloadShard(int shardNumber) throws IOException {

		{
			ZuliaShard s = primaryShardMap.remove(shardNumber);
			if (s != null) {
				LOG.info("{}:{} Closing primary shard {}:s{}", zuliaConfig.getServerAddress(), zuliaConfig.getServicePort(), indexName, shardNumber);
				s.close();
				LOG.info("{}:{} Removed primary shard {}:s{}", zuliaConfig.getServerAddress(), zuliaConfig.getServicePort(), indexName, shardNumber);

			}
		}

		{
			ZuliaShard s = replicaShardMap.remove(shardNumber);
			if (s != null) {
				LOG.info("{}:{} Closing replica shard {}:s{}", zuliaConfig.getServerAddress(), zuliaConfig.getServicePort(), indexName, shardNumber);
				s.close();
				LOG.info("{}:{} Removed replica shard {}:s{}", zuliaConfig.getServerAddress(), zuliaConfig.getServicePort(), indexName, shardNumber);
			}
		}

	}

	public void deleteIndex(boolean deleteAssociated) throws Exception {

		unload(true);

		if (deleteAssociated) {
			LOG.info("Dropping document storage.");
			documentStorage.drop();
			LOG.info("Dropped document storage.");
		}

	}

	public StoreResponse internalStore(StoreRequest storeRequest) throws Exception {

		long timestamp = System.currentTimeMillis();

		String uniqueId = storeRequest.getUniqueId();

		if (storeRequest.hasResultDocument()) {
			ResultDocument resultDocument = storeRequest.getResultDocument();
			DocumentContainer document = new DocumentContainer(resultDocument.getDocument());
			DocumentContainer metadata = new DocumentContainer(resultDocument.getMetadata());

			ZuliaShard s = findShardFromUniqueId(uniqueId);
			s.index(uniqueId, timestamp, document, metadata);

		}

		if (storeRequest.getClearExistingAssociated()) {
			documentStorage.deleteAssociatedDocuments(uniqueId);
		}

		for (AssociatedDocument ad : storeRequest.getAssociatedDocumentList()) {
			ad = AssociatedDocument.newBuilder(ad).setTimestamp(timestamp).build();
			documentStorage.storeAssociatedDocument(ad);
		}

		for (ZuliaBase.ExternalDocument ed : storeRequest.getExternalDocumentList()) {
			ed = ZuliaBase.ExternalDocument.newBuilder(ed).setTimestamp(timestamp).build();
			documentStorage.registerExternalDocument(ed);
		}

		return StoreResponse.newBuilder().build();

	}

	private ZuliaShard findShardFromUniqueId(String uniqueId) throws ShardDoesNotExistException {
		int shardNumber = ShardUtil.findShardForUniqueId(uniqueId, numberOfShards);
		ZuliaShard zuliaShard = primaryShardMap.get(shardNumber);
		if (zuliaShard == null) {
			throw new ShardDoesNotExistException(indexName, shardNumber);
		}
		return zuliaShard;
	}

	public DeleteResponse deleteDocument(DeleteRequest deleteRequest) throws Exception {

		String uniqueId = deleteRequest.getUniqueId();

		if (deleteRequest.getDeleteDocument()) {
			ZuliaShard s = findShardFromUniqueId(deleteRequest.getUniqueId());
			s.deleteDocument(uniqueId);
		}

		if (deleteRequest.getDeleteAllAssociated()) {
			documentStorage.deleteAssociatedDocuments(uniqueId);
		}
		else if (!deleteRequest.getFilename().isEmpty()) {
			String fileName = deleteRequest.getFilename();
			documentStorage.deleteAssociatedDocument(uniqueId, fileName);
		}

		return DeleteResponse.getDefaultInstance();

	}

	public List<DeleteResponse> internalShardBatchDelete(InternalShardBatchDeleteRequest shardRequest) throws Exception {
		int shardNumber = shardRequest.getShardNumber();
		ZuliaShard shard = primaryShardMap.get(shardNumber);
		if (shard == null) {
			throw new ShardDoesNotExistException(indexName, shardNumber);
		}

		List<String> uniqueIds = shardRequest.getUniqueIdList();
		boolean deleteDocument = shardRequest.getDeleteDocument();
		boolean deleteAllAssociated = shardRequest.getDeleteAllAssociated();
		String filename = shardRequest.getFilename();

		List<DeleteResponse> responses = new ArrayList<>(uniqueIds.size());
		for (String uniqueId : uniqueIds) {
			if (deleteDocument) {
				shard.deleteDocument(uniqueId);
			}

			if (deleteAllAssociated) {
				documentStorage.deleteAssociatedDocuments(uniqueId);
			}
			else if (!filename.isEmpty()) {
				documentStorage.deleteAssociatedDocument(uniqueId, filename);
			}

			responses.add(DeleteResponse.getDefaultInstance());
		}
		return responses;
	}

	public Query handleTermQuery(ZuliaQuery.Query query) {

		if (query.getQfList().isEmpty()) {
			throw new IllegalArgumentException("Term query must give at least one query field (qf)");
		}
		else if (query.getQfList().size() == 1) {
			return getTermInSetQuery(query, query.getQfList().getFirst());
		}
		else {
			BooleanQuery.Builder booleanQueryBuilder = new BooleanQuery.Builder();
			booleanQueryBuilder.setMinimumNumberShouldMatch(query.getMm());
			for (String field : query.getQfList()) {
				Query inSetQuery = getTermInSetQuery(query, field);
				booleanQueryBuilder.add(inSetQuery, BooleanClause.Occur.SHOULD);
			}
			return booleanQueryBuilder.build();
		}

	}

	private Query getTermInSetQuery(ZuliaQuery.Query query, String field) {
		IndexFieldInfo indexFieldInfo = indexConfig.getIndexFieldInfo(field);

		if (indexFieldInfo == null) {
			throw new RuntimeException("Field " + field + " is not indexed");
		}

		return SetQueryHelper.getTermInSetQuery(query.getTermList(), field, indexFieldInfo);
	}

	public Query handleNumericSetQuery(ZuliaQuery.Query query) {
		ProtocolStringList qfList = query.getQfList();
		if (qfList.isEmpty()) {
			throw new IllegalArgumentException("Numeric set query must give at least one query field (qf)");
		}
		else if (qfList.size() == 1) {
			return getNumericSetQuery(query.getNumericSet(), qfList.getFirst());
		}
		else {
			BooleanQuery.Builder booleanQueryBuilder = new BooleanQuery.Builder();
			booleanQueryBuilder.setMinimumNumberShouldMatch(query.getMm());
			for (String field : qfList) {
				Query inSetQuery = getNumericSetQuery(query.getNumericSet(), field);
				booleanQueryBuilder.add(inSetQuery, BooleanClause.Occur.SHOULD);
			}
			return booleanQueryBuilder.build();
		}

	}

	private Query getNumericSetQuery(ZuliaQuery.NumericSet numericSet, String field) {
		IndexFieldInfo indexFieldInfo = indexConfig.getIndexFieldInfo(field);
		return SetQueryHelper.getNumericSetQuery(field, indexFieldInfo, numericSet::getIntegerValueList, numericSet::getLongValueList,
				numericSet::getFloatValueList, numericSet::getDoubleValueList);
	}

	public Query handleVectorQuery(ZuliaQuery.Query query) throws Exception {

		if (query.getVectorList().isEmpty()) {
			throw new IllegalArgumentException("Vector must not be empty for cosine sim query");
		}

		float[] vector = Floats.toArray(query.getVectorList());

		if (query.getQfList().isEmpty()) {
			throw new IllegalArgumentException("Cosine sim query must give at least one query field (qf)");
		}
		else if (query.getQfList().size() == 1) {
			return new KnnFloatVectorQuery(query.getQfList().getFirst(), vector, query.getVectorTopN(), getPreFilter(query.getVectorPreQueryList()));
		}
		else {
			BooleanQuery.Builder booleanQueryBuilder = new BooleanQuery.Builder();
			BooleanQuery preFilter = getPreFilter(query.getVectorPreQueryList());
			for (String field : query.getQfList()) {
				KnnFloatVectorQuery knnVectorQuery = new KnnFloatVectorQuery(field, vector, query.getVectorTopN(), preFilter);
				booleanQueryBuilder.add(knnVectorQuery, BooleanClause.Occur.SHOULD);
			}
			return booleanQueryBuilder.build();
		}
	}

	public Query handleMoreLikeThisQuery(ZuliaQuery.Query query) {
		ZuliaQuery.MoreLikeThisParams mltParams = query.getMoreLikeThisParams();
		List<String> textFields = mltParams.getFieldList();
		List<String> likeTexts = mltParams.getLikeTextList();
		String vectorField = mltParams.getVectorField();
		List<Float> resolvedVector = mltParams.getResolvedVectorList();

		boolean hasLexical = !textFields.isEmpty() && !likeTexts.isEmpty();
		boolean hasVector = !vectorField.isEmpty() && !resolvedVector.isEmpty();

		if (!hasLexical && !hasVector) {
			throw new IllegalArgumentException("More-like-this query must have either text fields with like text, or a vector field with vectors");
		}

		Query lexicalQuery = null;
		if (hasLexical) {
			int minTermFreq = mltParams.getMinTermFreq() > 0 ? mltParams.getMinTermFreq() : 2;
			int maxQueryTerms = mltParams.getMaxQueryTerms() > 0 ? mltParams.getMaxQueryTerms() : 25;
			int minDocFreq = mltParams.getMinDocFreq() > 0 ? mltParams.getMinDocFreq() : 5;
			int maxDocFreqPct = mltParams.getMaxDocFreqPct() > 0 ? mltParams.getMaxDocFreqPct() : 25;
			int maxNumTokensParsed = mltParams.getMaxNumTokensParsed() > 0 ? mltParams.getMaxNumTokensParsed() : 5000;
			lexicalQuery = new MoreLikeThisLazyQuery(likeTexts, textFields, zuliaPerFieldAnalyzer, minTermFreq, maxQueryTerms, minDocFreq,
					mltParams.getMaxDocFreq(), maxDocFreqPct, mltParams.getMinWordLen(), mltParams.getMaxWordLen(), maxNumTokensParsed, query.getMm());
		}

		Query vectorQuery = null;
		if (hasVector) {
			// vectorTopN is defaulted by QueryRequestValidator when a vector field is set
			vectorQuery = new KnnFloatVectorQuery(vectorField, Floats.toArray(resolvedVector), mltParams.getVectorTopN());
		}

		Query mltQuery;
		if (lexicalQuery == null) {
			mltQuery = vectorQuery;
		}
		else if (vectorQuery == null) {
			mltQuery = lexicalQuery;
		}
		else {
			BooleanQuery.Builder combined = new BooleanQuery.Builder();
			combined.add(applyWeight(lexicalQuery, mltParams.getTextWeight()), BooleanClause.Occur.SHOULD);
			combined.add(applyWeight(vectorQuery, mltParams.getVectorWeight()), BooleanClause.Occur.SHOULD);
			mltQuery = combined.build();
		}

		List<String> sourceDocIds = mltParams.getDocumentIdList();
		if (!sourceDocIds.isEmpty() && !mltParams.getIncludeSourceDocs()) {
			BooleanQuery.Builder excludingSources = new BooleanQuery.Builder();
			excludingSources.add(mltQuery, BooleanClause.Occur.MUST);
			for (String docId : sourceDocIds) {
				excludingSources.add(new TermQuery(new Term(ZuliaFieldConstants.ID_FIELD, docId)), BooleanClause.Occur.MUST_NOT);
			}
			mltQuery = excludingSources.build();
		}

		return mltQuery;
	}

	private static Query applyWeight(Query query, float weight) {
		return weight > 0 && weight != 1.0f ? new BoostQuery(query, weight) : query;
	}

	private BooleanQuery getPreFilter(List<ZuliaQuery.Query> vectorPreQueryList) throws Exception {
		if (vectorPreQueryList.isEmpty()) {
			return null;
		}

		boolean allNegative = true;
		BooleanQuery.Builder booleanQueryBuilder = new BooleanQuery.Builder();
		for (ZuliaQuery.Query q : vectorPreQueryList) {
			if (!isNegativeQuery(q.getQueryType())) {
				allNegative = false;
			}
			booleanQueryBuilder.add(generateClause(q));
		}

		if (allNegative) {
			booleanQueryBuilder.add(new BooleanClause(new MatchAllDocsQuery(), BooleanClause.Occur.FILTER));
		}

		return booleanQueryBuilder.build();
	}

	public Query getQuery(QueryRequest qr) throws Exception {

		List<BooleanClause> clauses = new ArrayList<>();

		List<ZuliaQuery.Query> queryList = qr.getQueryList();

		boolean allNegativeOrEmpty = true;

		for (ZuliaQuery.Query query : queryList) {
			if (!isNegativeQuery(query.getQueryType())) {
				allNegativeOrEmpty = false;
				break;
			}
		}

		if (allNegativeOrEmpty) {
			clauses.add(new BooleanClause(new MatchAllDocsQuery(), BooleanClause.Occur.FILTER));
		}

		for (ZuliaQuery.Query query : queryList) {
			clauses.add(generateClause(query));
		}

		if (qr.hasFacetRequest()) {
			FacetRequest facetRequest = qr.getFacetRequest();

			List<ZuliaQuery.DrillDown> drillDownList = facetRequest.getDrillDownList();
			if (!drillDownList.isEmpty()) {

				BooleanQuery.Builder drillDownQueries = new BooleanQuery.Builder();

				boolean allNegative = true;
				for (ZuliaQuery.DrillDown drillDown : drillDownList) {
					if (!drillDown.getExclude()) {
						allNegative = false;
					}

					BooleanQuery.Builder drillDownQuery = new BooleanQuery.Builder();

					for (Facet facet : drillDown.getFacetValueList()) {

						String[] path = facet.getPathList().toArray(String[]::new);
						FacetLabel facetLabel = new FacetLabel(facet.getValue(), path);

						Query query = new ConstantScoreQuery(new TermQuery(
								new Term(ZuliaFieldConstants.FACET_DRILL_DOWN_FIELD, FacetsConfig.pathToString(drillDown.getLabel(), facetLabel.components))));
						drillDownQuery.add(query,
								ZuliaQuery.Query.Operator.OR.equals(drillDown.getOperator()) ? BooleanClause.Occur.SHOULD : BooleanClause.Occur.MUST);

					}
					drillDownQuery.setMinimumNumberShouldMatch(drillDown.getMm());
					drillDownQueries.add(
							new BooleanClause(drillDownQuery.build(), drillDown.getExclude() ? BooleanClause.Occur.MUST_NOT : BooleanClause.Occur.FILTER));
				}
				if (allNegative) {
					drillDownQueries.add(new BooleanClause(new MatchAllDocsQuery(), BooleanClause.Occur.FILTER));
				}

				clauses.add(new BooleanClause(drillDownQueries.build(), BooleanClause.Occur.FILTER));
			}

		}

		if (clauses.size() == 1) {
			return clauses.getFirst().query();
		}

		BooleanQuery.Builder booleanQuery = new BooleanQuery.Builder();
		for (BooleanClause clause : clauses) {
			booleanQuery.add(clause);
		}

		return booleanQuery.build();

	}

	private static boolean isNegativeQuery(QueryType queryType) {
		return queryType.equals(QueryType.FILTER_NOT) || queryType.equals(QueryType.TERMS_NOT) || queryType.equals(QueryType.NUMERIC_SET_NOT);
	}

	private BooleanClause generateClause(ZuliaQuery.Query query) throws Exception {
		BooleanClause.Occur occur = BooleanClause.Occur.FILTER;
		Query luceneQuery;
		if ((query.getQueryType() == QueryType.TERMS)) {
			luceneQuery = handleTermQuery(query);
		}
		else if (query.getQueryType() == QueryType.TERMS_NOT) {
			luceneQuery = handleTermQuery(query);
			occur = BooleanClause.Occur.MUST_NOT;
		}
		else if (query.getQueryType() == QueryType.NUMERIC_SET) {
			luceneQuery = handleNumericSetQuery(query);
			occur = BooleanClause.Occur.MUST;
		}
		else if (query.getQueryType() == QueryType.NUMERIC_SET_NOT) {
			luceneQuery = handleNumericSetQuery(query);
			occur = BooleanClause.Occur.MUST_NOT;
		}
		else if (query.getQueryType() == QueryType.VECTOR) {
			luceneQuery = handleVectorQuery(query);
			occur = BooleanClause.Occur.MUST;
		}
		else if (query.getQueryType() == QueryType.VECTOR_SHOULD) {
			luceneQuery = handleVectorQuery(query);
			occur = BooleanClause.Occur.SHOULD;
		}
		else if (query.getQueryType() == QueryType.MORE_LIKE_THIS) {
			luceneQuery = handleMoreLikeThisQuery(query);
			occur = BooleanClause.Occur.MUST;
		}
		else {
			luceneQuery = parseQueryToLucene(query);
			if (query.getQueryType() == QueryType.SCORE_MUST) {
				occur = BooleanClause.Occur.MUST;
			}
			else if (query.getQueryType() == QueryType.SCORE_SHOULD) {
				occur = BooleanClause.Occur.SHOULD;
			}
			else if (query.getQueryType() == QueryType.FILTER_NOT) {
				occur = BooleanClause.Occur.MUST_NOT;
			}
			//defaults to filter
		}

		String scoreFunction = query.getScoreFunction();
		if (!scoreFunction.isEmpty()) {
			luceneQuery = handleScoreFunction(query.getScoreFunction(), luceneQuery);
		}

		float boost = query.getBoost();
		if (boost != 0f && boost != 1f) {
			luceneQuery = new BoostQuery(luceneQuery, boost);
		}

		return new BooleanClause(luceneQuery, occur);
	}

	private FunctionScoreQuery handleScoreFunction(String scoreFunction, Query query) throws java.text.ParseException {

		SimpleBindings bindings = new SimpleBindings();

		// Pre-process geodist() expressions: replace with synthetic variable names
		// JavascriptCompiler only supports variable names, not function calls
		java.util.Map<String, GeoDistanceValuesSource> geoBindings = new java.util.HashMap<>();
		java.util.regex.Matcher geoMatcher = GeoDistUtil.findGeoDist(scoreFunction);
		StringBuilder processed = new StringBuilder();
		int geoIndex = 0;
		while (geoMatcher.find()) {
			String syntheticVar = "__geodist_" + geoIndex++;
			GeoDistUtil.GeoDistParsed parsed = GeoDistUtil.parseGeoDist(geoMatcher.group());
			SortFieldInfo geoSortFieldInfo = indexConfig.getSortFieldInfo(parsed.field());
			if (geoSortFieldInfo == null) {
				throw new IllegalArgumentException(
						"geodist() field <" + parsed.field() + "> is not a sortable field. Add .sort() or .sortAs() to the field config to use geodist().");
			}
			if (!FieldTypeUtil.isGeoPointFieldType(geoSortFieldInfo.getFieldType())) {
				throw new IllegalArgumentException("geodist() field <" + parsed.field() + "> is not a GEO_POINT field");
			}
			geoBindings.put(syntheticVar,
					new GeoDistanceValuesSource(geoSortFieldInfo.getInternalSortFieldName(), parsed.latitude(), parsed.longitude()));
			// GeoDistanceValuesSource returns meters; convert to km for user-facing expressions
			geoMatcher.appendReplacement(processed, "(" + syntheticVar + " / 1000.0)");
		}
		geoMatcher.appendTail(processed);
		String compilableExpression = processed.toString();

		Expression expr = JavascriptCompiler.compile(compilableExpression);
		bindings.add(ZuliaFieldConstants.SCORE_FIELD, DoubleValuesSource.SCORES);
		for (String var : expr.variables) {
			if (ZuliaFieldConstants.SCORE_FIELD.equals(var)) {
				continue;
			}
			if (geoBindings.containsKey(var)) {
				bindings.add(var, geoBindings.get(var));
				continue;
			}
			SortFieldInfo sortFieldInfo = indexConfig.getSortFieldInfo(var);
			if (sortFieldInfo == null) {
				throw new IllegalArgumentException("Score Function references unknown sort field <" + var + ">");
			}
			FieldConfig.FieldType fieldType = sortFieldInfo.getFieldType();
			String internalFieldName = sortFieldInfo.getInternalSortFieldName();
			SortedNumericDoubleValuesSource valuesSource;
			if (FieldTypeUtil.isStoredAsInt(fieldType)) {
				valuesSource = SortedNumericDoubleValuesSource.fromInt(internalFieldName);
			}
			else if (FieldTypeUtil.isStoredAsLong(fieldType)) {
				valuesSource = SortedNumericDoubleValuesSource.fromLong(internalFieldName);
			}
			else if (FieldTypeUtil.isNumericFloatFieldType(fieldType)) {
				valuesSource = SortedNumericDoubleValuesSource.fromFloat(internalFieldName);
			}
			else if (FieldTypeUtil.isNumericDoubleFieldType(fieldType)) {
				valuesSource = SortedNumericDoubleValuesSource.fromDouble(internalFieldName);
			}
			else {
				throw new IllegalArgumentException("Score Function references sort field that is not numeric or date type <" + var + ">");
			}
			bindings.add(var, valuesSource);
		}

		return new FunctionScoreQuery(query, expr.getDoubleValuesSource(bindings));

	}

	private Query parseQueryToLucene(ZuliaQuery.Query zuliaQuery) throws Exception {

		try {
			ZuliaQuery.Query.Operator defaultOperator = zuliaQuery.getDefaultOp();
			String queryText = zuliaQuery.getQ();
			int minimumShouldMatchNumber = zuliaQuery.getMm();
			List<String> queryFields = zuliaQuery.getQfList();

			if (queryText.isEmpty()) {
				if (queryFields.isEmpty()) {
					return new MatchAllDocsQuery();
				}
				else {
					queryText = "*";
				}
			}

			Collection<String> defaultSearchFieldList;
			if (queryFields.isEmpty()) {
				defaultSearchFieldList = indexConfig.getIndexSettings().getDefaultSearchFieldList();
			}
			else {
				defaultSearchFieldList = queryFields;
			}

			Query query;

			query = parseWithQueryParser(queryText, minimumShouldMatchNumber, defaultOperator, defaultSearchFieldList);

			boolean negative = QueryUtil.isNegative(query);
			if (negative) {
				query = QueryUtil.fixNegativeQuery(query);
			}

			return query;
		}
		catch (ParseException e) {
			throw new IllegalArgumentException("Invalid Query: " + zuliaQuery.getQ());
		}
	}

	private Query parseWithQueryParser(String queryText, int minimumShouldMatchNumber, ZuliaQuery.Query.Operator defaultOperator,
			Collection<String> defaultSearchFieldList) throws Exception {
		Query query;
		ZuliaFlexibleQueryParser qp = parsers.pollFirst();
		if (qp == null) {
			qp = new ZuliaFlexibleQueryParser(zuliaPerFieldAnalyzer, indexConfig);
		}
		try {
			qp.setMinimumNumberShouldMatch(minimumShouldMatchNumber);
			qp.setDefaultOperator(defaultOperator);
			qp.setDefaultFields(defaultSearchFieldList);

			query = qp.parse(queryText);
		}
		finally {
			parsers.offerFirst(qp);
		}
		return query;
	}

	public IndexShardResponse internalQuery(Query query, final InternalQueryRequest internalQueryRequest) throws Exception {

		QueryRequest queryRequest = internalQueryRequest.getQueryRequest();
		Set<ZuliaShard> shardsForQuery = new HashSet<>();
		for (IndexRouting indexRouting : internalQueryRequest.getIndexRoutingList()) {
			if (indexRouting.getIndex().equals(indexName)) {
				List<ZuliaShard> shardsFromRouting = getShardsFromRouting(indexRouting, queryRequest.getPrimaryReplicaSettings());
				shardsForQuery.addAll(shardsFromRouting);
			}
		}

		ShardQuery shardQuery = getShardQuery(query, queryRequest, internalQueryRequest.getSearchId());
		// if concurrency is not set in the query default to the index concurrency if that is set or default to the node level config
		if (shardQuery.getConcurrency() == 0) {
			int indexDefaultConcurrency = indexConfig.getDefaultConcurrency();
			if (indexDefaultConcurrency != 0) {
				shardQuery.setConcurrency(indexDefaultConcurrency);
			}
			else {
				int nodeLevelDefaultConcurrency = zuliaConfig.getDefaultConcurrency();
				shardQuery.setConcurrency(nodeLevelDefaultConcurrency);
			}

		}

		IndexShardResponse.Builder builder = IndexShardResponse.newBuilder();

		List<Future<ShardQueryResponse>> responses = new ArrayList<>();

		for (final ZuliaShard shard : shardsForQuery) {
			Future<ShardQueryResponse> response = shardPool.submit(() -> shard.queryShard(shardQuery));
			responses.add(response);
		}

		for (Future<ShardQueryResponse> response : responses) {
			try {
				ShardQueryResponse rs = response.get();
				builder.addShardQueryResponse(rs);
			}
			catch (ExecutionException e) {
				Throwable t = e.getCause();

				if (t instanceof OutOfMemoryError) {
					throw (OutOfMemoryError) t;
				}

				throw ((Exception) e.getCause());
			}
		}

		builder.setIndexName(indexName);
		return builder.build();

	}

	public ShardQuery getShardQuery(Query query, QueryRequest queryRequest, long searchId) throws Exception {

		int amount = queryRequest.getAmount() + queryRequest.getStart();

		if (indexConfig.getNumberOfShards() != 1) {
			if (!queryRequest.getFetchFull() && (amount > 0)) {
				amount = (int) (((amount / (float) numberOfShards) + indexConfig.getIndexSettings().getMinShardRequest()) * indexConfig.getIndexSettings()
						.getRequestFactor());
			}
		}

		final int requestedAmount = amount;

		final HashMap<Integer, FieldDoc> lastScoreDocMap = new HashMap<>();
		FieldDoc after;

		ZuliaQuery.LastResult lr = queryRequest.getLastResult();
		for (ZuliaQuery.LastIndexResult lir : lr.getLastIndexResultList()) {
			if (indexName.equals(lir.getIndexName())) {
				for (ZuliaQuery.ScoredResult sr : lir.getLastForShardList()) {
					int luceneShardId = sr.getLuceneShardId();
					float score = sr.getScore();

					SortRequest sortRequest = queryRequest.getSortRequest();

					Object[] sortTerms = new Object[sortRequest.getFieldSortCount()];

					int sortTermsIndex = 0;

					ZuliaQuery.SortValues sortValues = sr.getSortValues();
					for (ZuliaQuery.FieldSort fs : sortRequest.getFieldSortList()) {

						String sortField = fs.getSortField();
						SortFieldInfo sortFieldInfo = indexConfig.getSortFieldInfo(sortField);

						if (sortFieldInfo == null) {
							throw new Exception(sortField + " is not defined as a sortable field");
						}

						FieldConfig.FieldType fieldType = sortFieldInfo.getFieldType();

						ZuliaQuery.SortValue sortValue = sortValues.getSortValue(sortTermsIndex);

						if (ZuliaFieldConstants.SCORE_FIELD.equals(sortField)) {
							sortTerms[sortTermsIndex] = sortValue.getFloatValue();
						}
						else if (sortValue.getExists()) {
							if (FieldTypeUtil.isHandledAsNumericFieldType(fieldType)) {
								if (FieldTypeUtil.isStoredAsInt(fieldType)) {
									sortTerms[sortTermsIndex] = sortValue.getIntegerValue();
								}
								else if (FieldTypeUtil.isNumericLongFieldType(fieldType)) {
									sortTerms[sortTermsIndex] = sortValue.getLongValue();
								}
								else if (FieldTypeUtil.isDateFieldType(fieldType)) {
									//TODO should this just use the LongValue and isStoredAsLong(fieldType) above (see note in ShardReader too)
									sortTerms[sortTermsIndex] = sortValue.getDateValue();
								}
								else if (FieldTypeUtil.isNumericFloatFieldType(fieldType)) {
									sortTerms[sortTermsIndex] = sortValue.getFloatValue();
								}
								else if (FieldTypeUtil.isNumericDoubleFieldType(fieldType)) {
									sortTerms[sortTermsIndex] = sortValue.getDoubleValue();
								}
								else {
									throw new Exception("Invalid numeric sort type <" + fieldType + "> for sort field <" + sortField + ">");
								}
							}
							else { //string
								sortTerms[sortTermsIndex] = new BytesRef(sortValue.getStringValue());
							}
						}
						else {
							sortTerms[sortTermsIndex] = null;
						}

						sortTermsIndex++;
					}

					after = new FieldDoc(luceneShardId, score, sortTerms, sr.getShard());
					lastScoreDocMap.put(sr.getShard(), after);
				}
			}
		}

		Map<String, Similarity> fieldSimilarityMap = new HashMap<>();
		for (FieldSimilarity fieldSimilarity : queryRequest.getFieldSimilarityList()) {
			fieldSimilarityMap.put(fieldSimilarity.getField(), fieldSimilarity.getSimilarity());
		}

		QueryCacheKey queryCacheKey = queryRequest.getDontCache() ? null : new QueryCacheKey(queryRequest);
		boolean debug = queryRequest.getDebug();
		return new ShardQuery(query, fieldSimilarityMap, requestedAmount, lastScoreDocMap, queryRequest.getFacetRequest(), queryRequest.getSortRequest(),
				queryCacheKey, queryRequest.getResultFetchType(), queryRequest.getDocumentFieldsList(), queryRequest.getDocumentMaskedFieldsList(),
				queryRequest.getHighlightRequestList(), queryRequest.getAnalysisRequestList(), debug, searchId, queryRequest.getSearchLabel(),
				queryRequest.getRealtime(), queryRequest.getConcurrency());
	}

	public Integer getNumberOfShards() {
		return numberOfShards;
	}

	public double getShardTolerance() {
		return indexConfig.getIndexSettings().getShardTolerance();
	}

	public void reloadIndexSettings() throws Exception {

		IndexSettings indexSettings = indexService.getIndex(indexName);
		if (indexSettings == null) {
			throw new IndexDoesNotExistException(indexName);
		}

		indexConfig.configure(indexSettings);
		zuliaPerFieldAnalyzer.refresh();

		for (ZuliaShard s : primaryShardMap.values()) {
			try {
				s.updateIndexSettings();
			}
			catch (Exception ignored) {
			}
		}

		for (ZuliaShard s : replicaShardMap.values()) {
			try {
				s.updateIndexSettings();
			}
			catch (Exception ignored) {
			}
		}

	}

	public OptimizeResponse optimize(OptimizeRequest request) throws Exception {

		int maxNumberOfSegments = 1;
		if (request.getMaxNumberOfSegments() > 0) {
			maxNumberOfSegments = request.getMaxNumberOfSegments();
		}
		for (final ZuliaShard shard : primaryShardMap.values()) {
			shard.optimize(maxNumberOfSegments);
		}

		reloadIndexSettings();
		return OptimizeResponse.newBuilder().build();
	}

	public ReindexResponse reindex(@SuppressWarnings("unused") ReindexRequest request) throws IOException {
		for (final ZuliaShard shard : primaryShardMap.values()) {
			shard.reindex();
		}
		return ReindexResponse.newBuilder().build();
	}

	public GetNumberOfDocsResponse getNumberOfDocs(InternalGetNumberOfDocsRequest request) throws Exception {

		List<Future<ShardCountResponse>> responses = new ArrayList<>();

		GetNumberOfDocsRequest getNumberOfDocsRequest = request.getGetNumberOfDocsRequest();

		List<ZuliaShard> shardsForCommand = getShardsFromRouting(request.getIndexRouting(), getNumberOfDocsRequest.getPrimaryReplicaSettings());

		for (ZuliaShard shard : shardsForCommand) {
			Future<ShardCountResponse> response = shardPool.submit(() -> shard.getNumberOfDocs(getNumberOfDocsRequest.getRealtime()));
			responses.add(response);
		}

		GetNumberOfDocsResponse.Builder responseBuilder = GetNumberOfDocsResponse.newBuilder();

		responseBuilder.setNumberOfDocs(0);
		for (Future<ShardCountResponse> response : responses) {
			try {
				ShardCountResponse scr = response.get();
				responseBuilder.addShardCountResponse(scr);
				responseBuilder.setNumberOfDocs(responseBuilder.getNumberOfDocs() + scr.getNumberOfDocs());
				responseBuilder.setSizeOnDiskBytes(responseBuilder.getSizeOnDiskBytes() + scr.getSizeOnDiskBytes());
			}
			catch (InterruptedException e) {
				throw new Exception("Interrupted while waiting for shard results");
			}
			catch (Exception e) {
				Throwable cause = e.getCause();
				if (cause instanceof Exception) {
					throw e;
				}

				throw e;
			}
		}

		return responseBuilder.build();

	}

	private List<ZuliaShard> getShardsFromRouting(IndexRouting indexRouting, PrimaryReplicaSettings primaryReplicaSettings) throws ShardDoesNotExistException {
		List<ZuliaShard> shardsForCommand = new ArrayList<>();

		for (int shardNumber : indexRouting.getShardList()) {

			ZuliaShard shard;

			if (PrimaryReplicaSettings.PRIMARY_ONLY.equals(primaryReplicaSettings)) {
				shard = primaryShardMap.get(shardNumber);
			}
			else if (PrimaryReplicaSettings.REPLICA_ONLY.equals(primaryReplicaSettings)) {
				shard = replicaShardMap.get(shardNumber);
			}
			else {
				shard = primaryShardMap.get(shardNumber);
				if (shard == null) {
					shard = replicaShardMap.get(shardNumber);
				}
			}

			if (shard == null) {
				throw new ShardDoesNotExistException(indexName, shardNumber);
			}

			shardsForCommand.add(shard);
		}
		return shardsForCommand;
	}

	public GetFieldNamesResponse getFieldNames(InternalGetFieldNamesRequest request) throws Exception {

		List<Future<GetFieldNamesResponse>> responses = new ArrayList<>();

		GetFieldNamesRequest getFieldNamesRequest = request.getGetFieldNamesRequest();

		List<ZuliaShard> shardsForCommand = getShardsFromRouting(request.getIndexRouting(), getFieldNamesRequest.getPrimaryReplicaSettings());

		for (final ZuliaShard shard : shardsForCommand) {

			Future<GetFieldNamesResponse> response = shardPool.submit(() -> shard.getFieldNames(getFieldNamesRequest.getRealtime()));

			responses.add(response);

		}

		GetFieldNamesResponse.Builder responseBuilder = GetFieldNamesResponse.newBuilder();

		Set<String> fields = new HashSet<>();
		for (Future<GetFieldNamesResponse> response : responses) {
			try {
				GetFieldNamesResponse gfnr = response.get();
				fields.addAll(gfnr.getFieldNameList());
			}
			catch (ExecutionException e) {
				Throwable cause = e.getCause();
				if (cause instanceof Exception) {
					throw e;
				}
				else {
					throw new Exception(cause);
				}
			}
		}

		fields.remove(ZuliaFieldConstants.TIMESTAMP_FIELD);
		fields.remove(ZuliaFieldConstants.STORED_ID_FIELD);
		fields.remove(ZuliaFieldConstants.STORED_DOC_FIELD);
		fields.remove(ZuliaFieldConstants.STORED_META_FIELD);
		fields.remove(ZuliaFieldConstants.ID_FIELD);
		fields.remove(ZuliaFieldConstants.FIELDS_LIST_FIELD);

		List<String> toRemove = new ArrayList<>();
		for (String field : fields) {
			if (field.startsWith(ZuliaFieldConstants.FACET_DRILL_DOWN_FIELD)) {
				toRemove.add(field);
			}
			if (field.startsWith(ZuliaFieldConstants.FACET_STORAGE)) {
				toRemove.add(field);
			}
			else if (FieldTypeUtil.isCharLengthField(field)) {
				toRemove.add(field);
			}
			else if (FieldTypeUtil.isListLengthField(field)) {
				toRemove.add(field);
			}
			else if (field.contains(ZuliaFieldConstants.SORT_SUFFIX)) {
				toRemove.add(field);
			}
		}
		toRemove.forEach(fields::remove);

		responseBuilder.addAllFieldName(fields);
		return responseBuilder.build();

	}

	public ClearResponse clear(@SuppressWarnings("unused") ZuliaServiceOuterClass.ClearRequest request) throws Exception {

		List<Future<Void>> responses = new ArrayList<>();

		for (final ZuliaShard shard : primaryShardMap.values()) {

			Future<Void> response = shardPool.submit(() -> {
				shard.clear();
				return null;
			});

			responses.add(response);

		}

		for (Future<Void> response : responses) {
			try {
				response.get();
			}
			catch (ExecutionException e) {
				Throwable cause = e.getCause();
				if (cause instanceof Exception) {
					throw e;
				}
				else {
					throw new Exception(cause);
				}
			}
		}

		documentStorage.deleteAllDocuments();

		return ClearResponse.newBuilder().build();

	}

	public InternalGetTermsResponse getTerms(final InternalGetTermsRequest request) throws Exception {

		List<Future<GetTermsResponse>> responses = new ArrayList<>();

		GetTermsRequest getTermsRequest = request.getGetTermsRequest();
		List<ZuliaShard> shardsForCommand = getShardsFromRouting(request.getIndexRouting(), getTermsRequest.getPrimaryReplicaSettings());

		for (final ZuliaShard shard : shardsForCommand) {

			Future<GetTermsResponse> response = shardPool.submit(() -> shard.getTerms(getTermsRequest));

			responses.add(response);

		}

		InternalGetTermsResponse.Builder getTermsResponseInternalBuilder = InternalGetTermsResponse.newBuilder();
		for (Future<GetTermsResponse> response : responses) {
			try {
				GetTermsResponse gtr = response.get();
				getTermsResponseInternalBuilder.addGetTermsResponse(gtr);
			}
			catch (ExecutionException e) {
				Throwable cause = e.getCause();
				if (cause instanceof Exception) {
					throw e;
				}
				else {
					throw new Exception(cause);
				}
			}
		}

		return getTermsResponseInternalBuilder.build();

	}

	public OutputStream getAssociatedDocumentOutputStream(String uniqueId, String fileName, long clusterTime, Document metadata) throws Exception {
		return documentStorage.getAssociatedDocumentOutputStream(uniqueId, fileName, clusterTime, metadata);
	}

	public InputStream getAssociatedDocumentStream(String uniqueId, String fileName) throws Exception {

		return documentStorage.getAssociatedDocumentStream(uniqueId, fileName);

	}

	public List<String> getAssociatedFilenames(String uniqueId) throws Exception {
		return documentStorage.getAssociatedFilenames(uniqueId);
	}

	public Stream<AssociatedMetadataDTO> getAssociatedMetadataForQuery(Document query) {
		return documentStorage.getAssociatedMetadataForQuery(query);
	}

	private ResultDocument getSourceDocument(String uniqueId, FetchType resultFetchType, List<String> fieldsToReturn, List<String> fieldsToMask,
			boolean realtime) throws Exception {

		ZuliaShard s = findShardFromUniqueId(uniqueId);
		return s.getSourceDocument(uniqueId, resultFetchType, fieldsToReturn, fieldsToMask, realtime);

	}

	public List<FetchResponse> internalShardBatchFetch(InternalShardBatchFetchRequest shardRequest) throws Exception {
		int shardNumber = shardRequest.getShardNumber();
		ZuliaShard shard = primaryShardMap.get(shardNumber);
		if (shard == null) {
			throw new ShardDoesNotExistException(indexName, shardNumber);
		}

		List<String> uniqueIds = shardRequest.getUniqueIdList();
		FetchType resultFetchType = shardRequest.getResultFetchType();

		Map<String, ResultDocument> sourceDocMap = null;
		if (!FetchType.NONE.equals(resultFetchType)) {
			sourceDocMap = shard.getSourceDocuments(uniqueIds, resultFetchType, shardRequest.getDocumentFieldsList(),
					shardRequest.getDocumentMaskedFieldsList(), shardRequest.getRealtime());
		}

		FetchType associatedFetchType = shardRequest.getAssociatedFetchType();
		String filename = shardRequest.getFilename();

		List<FetchResponse> responses = new ArrayList<>(uniqueIds.size());
		for (String uniqueId : uniqueIds) {
			FetchResponse.Builder frBuilder = FetchResponse.newBuilder();

			if (sourceDocMap != null) {
				ResultDocument resultDoc = sourceDocMap.get(uniqueId);
				if (resultDoc != null) {
					frBuilder.setResultDocument(resultDoc);
				}
			}

			if (!FetchType.NONE.equals(associatedFetchType)) {
				if (!filename.isEmpty()) {
					AssociatedDocument ad = getAssociatedDocument(uniqueId, filename, associatedFetchType);
					if (ad != null) {
						frBuilder.addAssociatedDocument(ad);
					}
				}
				else {
					for (AssociatedDocument ad : getAssociatedMetadataForUniqueId(uniqueId, associatedFetchType)) {
						frBuilder.addAssociatedDocument(ad);
					}
				}
			}
			responses.add(frBuilder.build());
		}
		return responses;
	}

	public AssociatedDocument getAssociatedDocument(String uniqueId, String fileName, FetchType associatedFetchType) throws Exception {

		return documentStorage.getAssociatedDocument(uniqueId, fileName, associatedFetchType);

	}

	public List<AssociatedDocument> getAssociatedMetadataForUniqueId(String uniqueId, FetchType associatedFetchType) throws Exception {

		return documentStorage.getAssociatedMetadataForUniqueId(uniqueId, associatedFetchType);

	}

	public IndexShardMapping getIndexShardMapping() {
		return indexShardMapping;
	}

	public String getIndexName() {
		return indexName;
	}

	public ReplicaDirectoryApplier newReplicaApplier(int shardNumber) throws ShardDoesNotExistException {
		return new ReplicaDirectoryApplier(requireReplicaShard(shardNumber));
	}

	public List<ReplicaFileInfo> listReplicaFileInfo(int shardNumber, boolean taxonomy) throws IOException {
		ZuliaShard shard = requireReplicaShard(shardNumber);
		ShardReadManager readManager = shard.getShardReadManager();
		Directory directory = taxonomy ? readManager.getTaxoDirectory() : readManager.getIndexDirectory();
		List<ReplicaFileInfo> result = new ArrayList<>();
		for (String fileName : directory.listAll()) {
			ReplicaFileInfo info = ReplicationUtil.readFileInfo(directory, fileName);
			if (info != null) {
				result.add(info);
			}
		}
		return result;
	}

	private ZuliaShard requireReplicaShard(int shardNumber) throws ShardDoesNotExistException {
		ZuliaShard shard = replicaShardMap.get(shardNumber);
		if (shard == null || shard.getShardReadManager() == null) {
			throw new ShardDoesNotExistException(indexName, shardNumber);
		}
		return shard;
	}

	public List<ReplicationShardState> getReplicationState() {
		return segmentReplicationManager.getReplicationState();
	}

	public void applyShardMappingUpdate(IndexShardMapping newMapping, Predicate<Node> thisNodeTest) throws Exception {
		for (ShardMapping shardMapping : newMapping.getShardMappingList()) {
			int shardNumber = shardMapping.getShardNumber();
			boolean shouldBePrimary = thisNodeTest.test(shardMapping.getPrimaryNode());
			boolean shouldBeReplica = !shouldBePrimary && shardMapping.getReplicaNodeList().stream().anyMatch(thisNodeTest);

			boolean hasPrimary = primaryShardMap.containsKey(shardNumber);
			boolean hasReplica = replicaShardMap.containsKey(shardNumber);

			if (shouldBePrimary && !hasPrimary) {
				// promotion reuses the on-disk data, so unload the old replica role without deleting
				if (hasReplica) {
					unloadShard(shardNumber);
				}
				loadShard(shardNumber, true);
			}
			else if (shouldBeReplica && !hasReplica) {
				// demotion reuses the on-disk data, so unload the old primary role without deleting
				if (hasPrimary) {
					unloadShard(shardNumber);
				}
				loadShard(shardNumber, false);
			}
			else if (!shouldBePrimary && !shouldBeReplica) {
				if (hasReplica) {
					// this node is no longer a replica; the primary is the source of truth, so the local copy is disposable
					unloadShard(shardNumber);
					deleteShardData(shardNumber);
				}
				else if (hasPrimary) {
					// primary moved off this node entirely; keep the data on disk rather than risk losing the only copy
					unloadShard(shardNumber);
				}
			}
		}

		this.indexShardMapping = newMapping;
		segmentReplicationManager.updateShardMapping(newMapping);

		for (ZuliaShard primary : primaryShardMap.values()) {
			segmentReplicationManager.replicateAfterCommit(primary);
		}
	}

	public void loadShards(Predicate<Node> thisNodeTest) throws Exception {
		List<ShardMapping> shardMappingList = indexShardMapping.getShardMappingList();
		for (ShardMapping shardMapping : shardMappingList) {
			if (thisNodeTest.test(shardMapping.getPrimaryNode())) {
				loadShard(shardMapping.getShardNumber(), true);
			}
			else {
				for (Node node : shardMapping.getReplicaNodeList()) {
					if (thisNodeTest.test(node)) {
						loadShard(shardMapping.getShardNumber(), false);
						break;
					}
				}

			}
		}

		// Catch up replicas that missed commits while this primary was down. replicateAfterCommit
		// short-circuits when there are no replicas or nothing has changed, so this is cheap.
		for (ZuliaShard primary : primaryShardMap.values()) {
			segmentReplicationManager.replicateAfterCommit(primary);
		}
	}

	public ZuliaServiceOuterClass.FetchResponse fetch(ZuliaServiceOuterClass.FetchRequest fetchRequest) throws Exception {
		ZuliaServiceOuterClass.FetchResponse.Builder frBuilder = ZuliaServiceOuterClass.FetchResponse.newBuilder();

		String uniqueId = fetchRequest.getUniqueId();

		FetchType resultFetchType = fetchRequest.getResultFetchType();
		if (!FetchType.NONE.equals(resultFetchType)) {

			ZuliaBase.ResultDocument resultDoc = getSourceDocument(uniqueId, resultFetchType, fetchRequest.getDocumentFieldsList(),
					fetchRequest.getDocumentMaskedFieldsList(), fetchRequest.getRealtime());
			if (null != resultDoc) {
				frBuilder.setResultDocument(resultDoc);
			}
		}

		FetchType associatedFetchType = fetchRequest.getAssociatedFetchType();
		if (!FetchType.NONE.equals(associatedFetchType)) {
			if (!fetchRequest.getFilename().isEmpty()) {
				AssociatedDocument ad = getAssociatedDocument(uniqueId, fetchRequest.getFilename(), associatedFetchType);
				if (ad != null) {
					frBuilder.addAssociatedDocument(ad);
				}
			}
			else {
				for (AssociatedDocument ad : getAssociatedMetadataForUniqueId(uniqueId, associatedFetchType)) {
					frBuilder.addAssociatedDocument(ad);
				}
			}
		}
		return frBuilder.build();
	}

	public ServerIndexConfig getIndexConfig() {
		return indexConfig;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;

		ZuliaIndex that = (ZuliaIndex) o;

		return indexName.equals(that.indexName);
	}

	@Override
	public int hashCode() {
		return indexName.hashCode();
	}

	public ZuliaBase.IndexStats getIndexStats() throws IOException {

		ZuliaBase.IndexStats.Builder indexStats = ZuliaBase.IndexStats.newBuilder();
		indexStats.setIndexName(indexName);
		for (ZuliaShard zuliaShard : primaryShardMap.values()) {
			indexStats.addShardCacheStat(zuliaShard.getShardCacheStats());
		}
		for (ZuliaShard zuliaShard : replicaShardMap.values()) {
			indexStats.addShardCacheStat(zuliaShard.getShardCacheStats());
		}

		return indexStats.build();
	}
}
