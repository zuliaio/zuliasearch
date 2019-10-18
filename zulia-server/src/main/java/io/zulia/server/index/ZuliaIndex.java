package io.zulia.server.index;

import info.debatty.java.lsh.SuperBit;
import io.zulia.ZuliaConstants;
import io.zulia.message.ZuliaBase;
import io.zulia.message.ZuliaBase.AssociatedDocument;
import io.zulia.message.ZuliaBase.MasterSlaveSettings;
import io.zulia.message.ZuliaBase.Node;
import io.zulia.message.ZuliaBase.ResultDocument;
import io.zulia.message.ZuliaBase.ShardCountResponse;
import io.zulia.message.ZuliaBase.Similarity;
import io.zulia.message.ZuliaIndex.FieldConfig;
import io.zulia.message.ZuliaIndex.IndexMapping;
import io.zulia.message.ZuliaIndex.IndexSettings;
import io.zulia.message.ZuliaIndex.ShardMapping;
import io.zulia.message.ZuliaQuery;
import io.zulia.message.ZuliaQuery.CosineSimRequest;
import io.zulia.message.ZuliaQuery.Facet;
import io.zulia.message.ZuliaQuery.FacetRequest;
import io.zulia.message.ZuliaQuery.FetchType;
import io.zulia.message.ZuliaQuery.FieldSimilarity;
import io.zulia.message.ZuliaQuery.IndexShardResponse;
import io.zulia.message.ZuliaQuery.ShardQueryResponse;
import io.zulia.message.ZuliaQuery.SortRequest;
import io.zulia.message.ZuliaServiceOuterClass;
import io.zulia.message.ZuliaServiceOuterClass.*;
import io.zulia.server.analysis.ZuliaPerFieldAnalyzer;
import io.zulia.server.config.IndexService;
import io.zulia.server.config.ServerIndexConfig;
import io.zulia.server.config.ZuliaConfig;
import io.zulia.server.exceptions.ShardDoesNotExistException;
import io.zulia.server.field.FieldTypeUtil;
import io.zulia.server.filestorage.DocumentStorage;
import io.zulia.server.search.QueryCacheKey;
import io.zulia.server.search.ZuliaMultiFieldQueryParser;
import io.zulia.server.util.DeletingFileVisitor;
import io.zulia.util.ZuliaThreadFactory;
import io.zulia.util.ZuliaUtil;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.lucene.facet.DrillDownQuery;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.bson.Document;

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
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ZuliaIndex {

	private final static Logger LOG = Logger.getLogger(ZuliaIndex.class.getSimpleName());

	private final ServerIndexConfig indexConfig;

	private final GenericObjectPool<ZuliaMultiFieldQueryParser> parsers;
	private final ConcurrentHashMap<Integer, ZuliaShard> primaryShardMap;
	private final ConcurrentHashMap<Integer, ZuliaShard> replicaShardMap;

	private final ExecutorService shardPool;
	private final int numberOfShards;
	private final String indexName;

	private final DocumentStorage documentStorage;
	private final ZuliaConfig zuliaConfig;

	private Timer commitTimer;
	private TimerTask commitTask;
	private ZuliaPerFieldAnalyzer zuliaPerFieldAnalyzer;
	private final IndexService indexService;

	private IndexMapping indexMapping;
	private FacetsConfig facetsConfig;

	public ZuliaIndex(ZuliaConfig zuliaConfig, ServerIndexConfig indexConfig, DocumentStorage documentStorage, IndexService indexService) {

		this.zuliaConfig = zuliaConfig;
		this.indexConfig = indexConfig;
		this.indexName = indexConfig.getIndexName();
		this.numberOfShards = indexConfig.getNumberOfShards();
		this.indexService = indexService;

		this.facetsConfig = new FacetsConfig();
		this.facetsConfig.setIndexFieldName("myField", "$facets.float");

		this.documentStorage = documentStorage;

		this.shardPool = Executors.newCachedThreadPool(new ZuliaThreadFactory(indexName + "-shards"));

		this.zuliaPerFieldAnalyzer = new ZuliaPerFieldAnalyzer(indexConfig);

		this.parsers = new GenericObjectPool<>(new BasePooledObjectFactory<ZuliaMultiFieldQueryParser>() {

			@Override
			public ZuliaMultiFieldQueryParser create() {
				return new ZuliaMultiFieldQueryParser(zuliaPerFieldAnalyzer, ZuliaIndex.this.indexConfig);
			}

			@Override
			public PooledObject<ZuliaMultiFieldQueryParser> wrap(ZuliaMultiFieldQueryParser obj) {
				return new DefaultPooledObject<>(obj);
			}

		});

		this.primaryShardMap = new ConcurrentHashMap<>();
		this.replicaShardMap = new ConcurrentHashMap<>();

		commitTimer = new Timer(indexName + "-CommitTimer", true);

		commitTask = new TimerTask() {

			@Override
			public void run() {
				if (ZuliaIndex.this.indexConfig.getIndexSettings().getIdleTimeWithoutCommit() != 0) {
					doCommit(false);
				}

			}

		};

		commitTimer.scheduleAtFixedRate(commitTask, 1000, 1000);

	}

	public FieldConfig.FieldType getSortFieldType(String fieldName) {
		return indexConfig.getFieldTypeForSortField(fieldName);
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
				LOG.log(Level.SEVERE, "Failed to flush shard <" + shard.getShardNumber() + "> for index <" + indexName + ">", e);
			}
		}

	}

	public void unload(boolean terminate) throws IOException {

		LOG.info("Canceling timers for <" + indexName + ">");
		commitTask.cancel();
		commitTimer.cancel();

		if (!terminate) {
			LOG.info("Committing <" + indexName + ">");
			doCommit(true);
		}

		LOG.info("Shutting shard pool for <" + indexName + ">");
		shardPool.shutdownNow();

		for (Integer shardNumber : primaryShardMap.keySet()) {
			unloadShard(shardNumber);
			if (terminate) {
				Files.walkFileTree(getPathForIndex(shardNumber), new DeletingFileVisitor());
				Files.walkFileTree(getPathForFacetsIndex(shardNumber), new DeletingFileVisitor());
			}
		}

		for (Integer shardNumber : replicaShardMap.keySet()) {
			unloadShard(shardNumber);
			if (terminate) {
				Files.walkFileTree(getPathForIndex(shardNumber), new DeletingFileVisitor());
				Files.walkFileTree(getPathForFacetsIndex(shardNumber), new DeletingFileVisitor());
			}
		}

	}

	private void loadShard(int shardNumber, boolean primary) throws Exception {

		ShardWriteManager shardWriteManager = new ShardWriteManager(shardNumber, getPathForIndex(shardNumber), getPathForFacetsIndex(shardNumber), facetsConfig,
				indexConfig, zuliaPerFieldAnalyzer);

		ZuliaShard s = new ZuliaShard(shardWriteManager, primary);

		if (primary) {
			LOG.info("Loaded primary shard <" + shardNumber + "> for index <" + indexName + ">");
			primaryShardMap.put(shardNumber, s);
		}
		else {
			LOG.info("Loaded replica shard <" + shardNumber + "> for index <" + indexName + ">");
			replicaShardMap.put(shardNumber, s);
		}

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
				LOG.info(getLogPrefix() + "Closing primary shard <" + shardNumber + "> for index <" + indexName + ">");
				s.close();
				LOG.info(getLogPrefix() + "Removed primary shard <" + shardNumber + "> for index <" + indexName + ">");

			}
		}

		{
			ZuliaShard s = replicaShardMap.remove(shardNumber);
			if (s != null) {
				LOG.info(getLogPrefix() + "Closing replica shard <" + shardNumber + "> for index <" + indexName + ">");
				s.close();
				LOG.info(getLogPrefix() + "Removed replica shard <" + shardNumber + "> for index <" + indexName + ">");
			}
		}

	}

	public void deleteIndex() throws Exception {

		unload(true);

		documentStorage.drop();

	}

	public StoreResponse internalStore(StoreRequest storeRequest) throws Exception {

		long timestamp = System.currentTimeMillis();

		String uniqueId = storeRequest.getUniqueId();

		if (storeRequest.hasResultDocument()) {
			ResultDocument resultDocument = storeRequest.getResultDocument();
			Document document;
			if (resultDocument.getDocument() != null) {
				document = ZuliaUtil.byteArrayToMongoDocument(resultDocument.getDocument().toByteArray());
			}
			else {
				document = new Document();
			}
			Document metadata;
			if (resultDocument.getMetadata() != null) {
				metadata = ZuliaUtil.byteArrayToMongoDocument(resultDocument.getMetadata().toByteArray());
			}
			else {
				metadata = new Document();
			}

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

		return StoreResponse.newBuilder().build();

	}

	private ZuliaShard findShardFromUniqueId(String uniqueId) throws ShardDoesNotExistException {
		int shardNumber = MasterSlaveSelector.getShardForUniqueId(uniqueId, numberOfShards);
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
		else if (deleteRequest.getFilename() != null) {
			String fileName = deleteRequest.getFilename();
			documentStorage.deleteAssociatedDocument(uniqueId, fileName);
		}

		return DeleteResponse.newBuilder().build();

	}

	public BooleanQuery handleCosineSimQuery(CosineSimRequest cosineSimRequest) {

		double vector[] = new double[cosineSimRequest.getVectorCount()];
		for (int i = 0; i < cosineSimRequest.getVectorCount(); i++) {
			vector[i] = cosineSimRequest.getVector(i);
		}

		SuperBit superBit = indexConfig.getSuperBitForField(cosineSimRequest.getField());
		boolean[] signature = superBit.signature(vector);

		int mm = (int) ((1 - (Math.acos(cosineSimRequest.getSimilarity()) / Math.PI)) * signature.length);
		BooleanQuery.Builder booleanQueryBuilder = new BooleanQuery.Builder();
		booleanQueryBuilder.setMinimumNumberShouldMatch(mm);
		for (int i = 0; i < signature.length; i++) {
			String fieldName = ZuliaConstants.SUPERBIT_PREFIX + "." + cosineSimRequest.getField() + "." + i;
			booleanQueryBuilder
					.add(new BooleanClause(new TermQuery(new org.apache.lucene.index.Term(fieldName, signature[i] ? "1" : "0")), BooleanClause.Occur.SHOULD));
		}

		return booleanQueryBuilder.build();

	}

	public Query getQuery(QueryRequest qr) throws Exception {

		Query q = parseQueryToLucene(qr.getQuery());

		Map<String, Set<String>> facetToValues = new HashMap<>();
		if (qr.hasFacetRequest()) {
			FacetRequest facetRequest = qr.getFacetRequest();

			List<Facet> drillDownList = facetRequest.getDrillDownList();
			if (!drillDownList.isEmpty()) {
				for (Facet drillDown : drillDownList) {
					String key = drillDown.getLabel();
					String value = drillDown.getValue();
					if (!facetToValues.containsKey(key)) {
						facetToValues.put(key, new HashSet<>());
					}
					facetToValues.get(key).add(value);
				}
			}
		}

		if (!qr.getFilterQueryList().isEmpty() || !qr.getCosineSimRequestList().isEmpty() || !facetToValues.isEmpty() || !qr.getScoredQueryList().isEmpty()) {
			BooleanQuery.Builder booleanQuery = new BooleanQuery.Builder();
			for (ZuliaQuery.Query filterQuery : qr.getFilterQueryList()) {
				Query filterLuceneQuery = parseQueryToLucene(filterQuery);
				booleanQuery.add(filterLuceneQuery, BooleanClause.Occur.FILTER);
			}

			if (!facetToValues.isEmpty()) {
				DrillDownQuery drillDownQuery = new DrillDownQuery(facetsConfig);
				for (Map.Entry<String, Set<String>> entry : facetToValues.entrySet()) {
					String indexFieldName = entry.getKey();
					for (String value : entry.getValue()) {
						drillDownQuery.add(indexFieldName, value);
					}
				}
				booleanQuery.add(drillDownQuery, BooleanClause.Occur.FILTER);
			}

			for (CosineSimRequest cosineSimRequest : qr.getCosineSimRequestList()) {
				BooleanQuery cosineQuery = handleCosineSimQuery(cosineSimRequest);
				booleanQuery.add(cosineQuery, BooleanClause.Occur.MUST);
			}

			for (ZuliaQuery.Query scoredQuery : qr.getScoredQueryList()) {
				Query scoredLuceneQuery = parseQueryToLucene(scoredQuery);
				booleanQuery.add(scoredLuceneQuery, BooleanClause.Occur.MUST);
			}

			booleanQuery.add(q, BooleanClause.Occur.MUST);
			q = booleanQuery.build();
		}

		return q;

	}

	private Query parseQueryToLucene(ZuliaQuery.Query zuliaQuery) throws Exception {

		try {
			ZuliaQuery.Query.Operator defaultOperator = zuliaQuery.getDefaultOp();
			String queryText = zuliaQuery.getQ();
			Integer minimumShouldMatchNumber = zuliaQuery.getMm();
			List<String> queryFields = zuliaQuery.getQfList();

			QueryParser.Operator operator = null;
			if (defaultOperator.equals(ZuliaQuery.Query.Operator.OR)) {
				operator = QueryParser.Operator.OR;
			}
			else if (defaultOperator.equals(ZuliaQuery.Query.Operator.AND)) {
				operator = QueryParser.Operator.AND;
			}
			else {
				//this should never happen
				LOG.severe("Unknown operator type: <" + defaultOperator + ">");
			}

			ZuliaMultiFieldQueryParser qp = null;
			if (queryText == null || queryText.isEmpty()) {
				if (queryFields.isEmpty()) {
					return new MatchAllDocsQuery();
				}
				else {
					queryText = "*";
				}
			}
			try {
				qp = parsers.borrowObject();
				qp.setMinimumNumberShouldMatch(minimumShouldMatchNumber);
				qp.setDefaultOperator(operator);

				if (zuliaQuery.getDismax()) {
					qp.enableDismax(zuliaQuery.getDismaxTie());
				}
				else {
					qp.disableDismax();
				}

				if (queryFields.isEmpty()) {
					qp.setDefaultFields(indexConfig.getIndexSettings().getDefaultSearchFieldList());
				}
				else {
					qp.setDefaultFields(queryFields);
				}
				Query query = qp.parse(queryText);
				boolean negative = QueryUtil.isNegative(query);
				if (negative) {
					query = QueryUtil.fixNegativeQuery(query);
				}

				return query;

			}
			finally {
				parsers.returnObject(qp);
			}
		}
		catch (ParseException e) {
			throw new IllegalArgumentException("Invalid Query: " + zuliaQuery.getQ());
		}
	}

	public IndexShardResponse internalQuery(Query query, final InternalQueryRequest internalQueryRequest) throws Exception {

		QueryRequest queryRequest = internalQueryRequest.getQueryRequest();
		Set<ZuliaShard> shardsForQuery = new HashSet<>();
		for (IndexRouting indexRouting : internalQueryRequest.getIndexRoutingList()) {
			if (indexRouting.getIndex().equals(indexName)) {
				List<ZuliaShard> shardsFromRouting = getShardsFromRouting(indexRouting, queryRequest.getMasterSlaveSettings());
				shardsForQuery.addAll(shardsFromRouting);
			}
		}

		int amount = queryRequest.getAmount() + queryRequest.getStart();

		if (indexConfig.getNumberOfShards() != 1) {
			if (!queryRequest.getFetchFull() && (amount > 0)) {
				amount = (int) (((amount / numberOfShards) + indexConfig.getIndexSettings().getMinShardRequest()) * indexConfig.getIndexSettings()
						.getRequestFactor());
			}
		}

		final int requestedAmount = amount;

		final HashMap<Integer, FieldDoc> lastScoreDocMap = new HashMap<>();
		FieldDoc after;

		ZuliaQuery.LastResult lr = queryRequest.getLastResult();
		if (lr != null) {
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
							FieldConfig.FieldType sortType = indexConfig.getFieldTypeForSortField(sortField);
							if (sortType == null) {
								throw new Exception(sortField + " is not defined as a sortable field");
							}

							ZuliaQuery.SortValue sortValue = sortValues.getSortValue(sortTermsIndex);

							if (sortValue.getExists()) {
								if (FieldTypeUtil.isNumericOrDateFieldType(sortType)) {
									if (FieldTypeUtil.isNumericIntFieldType(sortType)) {
										sortTerms[sortTermsIndex] = sortValue.getIntegerValue();
									}
									else if (FieldTypeUtil.isNumericLongFieldType(sortType)) {
										sortTerms[sortTermsIndex] = sortValue.getLongValue();
									}
									else if (FieldTypeUtil.isNumericFloatFieldType(sortType)) {
										sortTerms[sortTermsIndex] = sortValue.getFloatValue();
									}
									else if (FieldTypeUtil.isNumericDoubleFieldType(sortType)) {
										sortTerms[sortTermsIndex] = sortValue.getDoubleValue();
									}
									else if (FieldTypeUtil.isDateFieldType(sortType)) {
										sortTerms[sortTermsIndex] = sortValue.getDateValue();
									}
									else {
										throw new Exception("Invalid numeric sort type <" + sortType + "> for sort field <" + sortField + ">");
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
		}

		Map<String, Similarity> fieldSimilarityMap = new HashMap<>();
		for (FieldSimilarity fieldSimilarity : queryRequest.getFieldSimilarityList()) {
			fieldSimilarityMap.put(fieldSimilarity.getField(), fieldSimilarity.getSimilarity());
		}

		for (CosineSimRequest cosineSimRequest : queryRequest.getCosineSimRequestList()) {
			io.zulia.message.ZuliaIndex.Superbit superbitConfig = indexConfig.getSuperBitConfigForField(cosineSimRequest.getField());

			int sigLength = superbitConfig.getInputDim() * superbitConfig.getBatches();
			for (int i = 0; i < sigLength; i++) {
				String fieldName = ZuliaConstants.SUPERBIT_PREFIX + "." + cosineSimRequest.getField() + "." + i;
				fieldSimilarityMap.put(fieldName, Similarity.CONSTANT);
			}

		}

		IndexShardResponse.Builder builder = IndexShardResponse.newBuilder();

		List<Future<ShardQueryResponse>> responses = new ArrayList<>();

		for (final ZuliaShard shard : shardsForQuery) {

			Future<ShardQueryResponse> response = shardPool.submit(() -> {

				QueryCacheKey queryCacheKey = null;

				if (!queryRequest.getDontCache()) {
					queryCacheKey = new QueryCacheKey(queryRequest);
				}

				return shard.queryShard(query, fieldSimilarityMap, requestedAmount, lastScoreDocMap.get(shard.getShardNumber()), queryRequest.getFacetRequest(),
						queryRequest.getSortRequest(), queryCacheKey, queryRequest.getResultFetchType(), queryRequest.getDocumentFieldsList(),
						queryRequest.getDocumentMaskedFieldsList(), queryRequest.getHighlightRequestList(), queryRequest.getAnalysisRequestList(),
						queryRequest.getDebug());
			});

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

	public Integer getNumberOfShards() {
		return numberOfShards;
	}

	public double getShardTolerance() {
		return indexConfig.getIndexSettings().getShardTolerance();
	}

	public void reloadIndexSettings() throws Exception {

		IndexSettings indexSettings = indexService.getIndex(indexName);
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

	public ReindexResponse reindex(ReindexRequest request) throws IOException {
		for (final ZuliaShard shard : primaryShardMap.values()) {
			shard.reindex();
		}
		return ReindexResponse.newBuilder().build();
	}

	public GetNumberOfDocsResponse getNumberOfDocs(InternalGetNumberOfDocsRequest request) throws Exception {

		List<Future<ShardCountResponse>> responses = new ArrayList<>();

		GetNumberOfDocsRequest getNumberOfDocsRequest = request.getGetNumberOfDocsRequest();

		List<ZuliaShard> shardsForCommand = getShardsFromRouting(request.getIndexRouting(), getNumberOfDocsRequest.getMasterSlaveSettings());

		for (ZuliaShard shard : shardsForCommand) {
			Future<ShardCountResponse> response = shardPool.submit(shard::getNumberOfDocs);
			responses.add(response);
		}

		GetNumberOfDocsResponse.Builder responseBuilder = GetNumberOfDocsResponse.newBuilder();

		responseBuilder.setNumberOfDocs(0);
		for (Future<ShardCountResponse> response : responses) {
			try {
				ShardCountResponse scr = response.get();
				responseBuilder.addShardCountResponse(scr);
				responseBuilder.setNumberOfDocs(responseBuilder.getNumberOfDocs() + scr.getNumberOfDocs());
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

	private List<ZuliaShard> getShardsFromRouting(IndexRouting indexRouting, MasterSlaveSettings masterSlaveSettings) throws ShardDoesNotExistException {
		List<ZuliaShard> shardsForCommand = new ArrayList<>();

		for (int shardNumber : indexRouting.getShardList()) {

			ZuliaShard shard;

			if (MasterSlaveSettings.MASTER_ONLY.equals(masterSlaveSettings)) {
				shard = primaryShardMap.get(shardNumber);
			}
			else if (MasterSlaveSettings.SLAVE_ONLY.equals(masterSlaveSettings)) {
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

		List<ZuliaShard> shardsForCommand = getShardsFromRouting(request.getIndexRouting(), getFieldNamesRequest.getMasterSlaveSettings());

		for (final ZuliaShard shard : shardsForCommand) {

			Future<GetFieldNamesResponse> response = shardPool.submit(shard::getFieldNames);

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

		fields.remove(ZuliaConstants.TIMESTAMP_FIELD);
		fields.remove(ZuliaConstants.STORED_DOC_FIELD);
		fields.remove(ZuliaConstants.STORED_META_FIELD);
		fields.remove(ZuliaConstants.ID_FIELD);
		fields.remove(ZuliaConstants.FIELDS_LIST_FIELD);

		List<String> toRemove = new ArrayList<>();
		for (String field : fields) {
			if (field.startsWith(FacetsConfig.DEFAULT_INDEX_FIELD_NAME)) {
				toRemove.add(field);
			}
			if (field.startsWith(ZuliaConstants.SUPERBIT_PREFIX)) {
				toRemove.add(field);
			}
		}
		fields.removeAll(toRemove);

		responseBuilder.addAllFieldName(fields);
		return responseBuilder.build();

	}

	public ClearResponse clear(ZuliaServiceOuterClass.ClearRequest request) throws Exception {

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
		List<ZuliaShard> shardsForCommand = getShardsFromRouting(request.getIndexRouting(), getTermsRequest.getMasterSlaveSettings());

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

	public void storeAssociatedDocument(String uniqueId, String fileName, InputStream is, long clusterTime, Document metadata) throws Exception {

		documentStorage.storeAssociatedDocument(uniqueId, fileName, is, clusterTime, metadata);

	}

	public InputStream getAssociatedDocumentStream(String uniqueId, String fileName) {

		return documentStorage.getAssociatedDocumentStream(uniqueId, fileName);

	}

	public void getAssociatedDocuments(OutputStream outputStream, Document filter) throws IOException {

		documentStorage.getAssociatedDocuments(outputStream, filter);

	}

	private ResultDocument getSourceDocument(String uniqueId, FetchType resultFetchType, List<String> fieldsToReturn, List<String> fieldsToMask)
			throws Exception {

		ZuliaShard s = findShardFromUniqueId(uniqueId);
		return s.getSourceDocument(uniqueId, resultFetchType, fieldsToReturn, fieldsToMask);

	}

	public AssociatedDocument getAssociatedDocument(String uniqueId, String fileName, FetchType associatedFetchType) throws Exception {

		return documentStorage.getAssociatedDocument(uniqueId, fileName, associatedFetchType);

	}

	public List<AssociatedDocument> getAssociatedDocuments(String uniqueId, FetchType associatedFetchType) throws Exception {

		return documentStorage.getAssociatedDocuments(uniqueId, associatedFetchType);

	}

	public void setIndexMapping(IndexMapping indexMapping) {
		this.indexMapping = indexMapping;
	}

	public IndexMapping getIndexMapping() {
		return indexMapping;
	}

	public String getIndexName() {
		return indexName;
	}

	public void loadShards(Predicate<Node> thisNodeTest) throws Exception {
		List<ShardMapping> shardMappingList = indexMapping.getShardMappingList();
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
	}

	public ZuliaServiceOuterClass.FetchResponse fetch(ZuliaServiceOuterClass.FetchRequest fetchRequest) throws Exception {
		ZuliaServiceOuterClass.FetchResponse.Builder frBuilder = ZuliaServiceOuterClass.FetchResponse.newBuilder();

		String uniqueId = fetchRequest.getUniqueId();

		FetchType resultFetchType = fetchRequest.getResultFetchType();
		if (!FetchType.NONE.equals(resultFetchType)) {

			ZuliaBase.ResultDocument resultDoc = getSourceDocument(uniqueId, resultFetchType, fetchRequest.getDocumentFieldsList(),
					fetchRequest.getDocumentMaskedFieldsList());
			if (null != resultDoc) {
				frBuilder.setResultDocument(resultDoc);
			}
		}

		FetchType associatedFetchType = fetchRequest.getAssociatedFetchType();
		if (!FetchType.NONE.equals(associatedFetchType)) {
			if (fetchRequest.getFilename() != null) {
				AssociatedDocument ad = getAssociatedDocument(uniqueId, fetchRequest.getFilename(), associatedFetchType);
				if (ad != null) {
					frBuilder.addAssociatedDocument(ad);
				}
			}
			else {
				for (AssociatedDocument ad : getAssociatedDocuments(uniqueId, associatedFetchType)) {
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

	private String getLogPrefix() {
		return zuliaConfig.getServerAddress() + ":" + zuliaConfig.getServicePort() + " ";
	}

}
