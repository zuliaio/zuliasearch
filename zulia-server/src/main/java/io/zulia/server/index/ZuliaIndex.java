package io.zulia.server.index;

import info.debatty.java.lsh.SuperBit;
import io.zulia.ZuliaConstants;
import io.zulia.message.ZuliaBase;
import io.zulia.message.ZuliaBase.AssociatedDocument;
import io.zulia.message.ZuliaBase.ResultDocument;
import io.zulia.message.ZuliaBase.ShardCountResponse;
import io.zulia.message.ZuliaBase.Similarity;
import io.zulia.message.ZuliaIndex.FieldConfig;
import io.zulia.message.ZuliaIndex.IndexMapping;
import io.zulia.message.ZuliaIndex.IndexSettings;
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
import io.zulia.message.ZuliaServiceOuterClass.ClearResponse;
import io.zulia.message.ZuliaServiceOuterClass.DeleteRequest;
import io.zulia.message.ZuliaServiceOuterClass.DeleteResponse;
import io.zulia.message.ZuliaServiceOuterClass.GetFieldNamesRequest;
import io.zulia.message.ZuliaServiceOuterClass.GetFieldNamesResponse;
import io.zulia.message.ZuliaServiceOuterClass.GetNumberOfDocsResponse;
import io.zulia.message.ZuliaServiceOuterClass.GetTermsRequest;
import io.zulia.message.ZuliaServiceOuterClass.GetTermsResponse;
import io.zulia.message.ZuliaServiceOuterClass.InternalGetTermsResponse;
import io.zulia.message.ZuliaServiceOuterClass.OptimizeRequest;
import io.zulia.message.ZuliaServiceOuterClass.OptimizeResponse;
import io.zulia.message.ZuliaServiceOuterClass.QueryRequest;
import io.zulia.message.ZuliaServiceOuterClass.StoreRequest;
import io.zulia.message.ZuliaServiceOuterClass.StoreResponse;
import io.zulia.server.config.IndexService;
import io.zulia.server.config.ServerIndexConfig;
import io.zulia.server.exceptions.ShardDoesNotExistException;
import io.zulia.server.filestorage.DocumentStorage;
import io.zulia.server.index.field.FieldTypeUtil;
import io.zulia.server.search.QueryCacheKey;
import io.zulia.server.search.ZuliaMultiFieldQueryParser;
import io.zulia.server.util.DeletingFileVisitor;
import io.zulia.util.ShardUtil;
import io.zulia.util.ZuliaThreadFactory;
import io.zulia.util.ZuliaUtil;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NRTCachingDirectory;
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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ZuliaIndex implements IndexShardInterface {

	private final static Logger LOG = Logger.getLogger(ZuliaIndex.class.getSimpleName());

	private final ServerIndexConfig indexConfig;

	private final GenericObjectPool<ZuliaMultiFieldQueryParser> parsers;
	private final ConcurrentHashMap<Integer, ZuliaShard> shardMap;
	private final ReadWriteLock indexLock;
	private final ExecutorService shardPool;
	private final int numberOfShards;
	private final String indexName;

	private final DocumentStorage documentStorage;

	private Timer commitTimer;
	private TimerTask commitTask;
	private ZuliaAnalyzerFactory analyzerFactory;
	private final IndexService indexService;

	static {
		FacetsConfig.DEFAULT_DIM_CONFIG.multiValued = true;
	}

	private IndexMapping indexMapping;

	public ZuliaIndex(ServerIndexConfig indexConfig, DocumentStorage documentStorage, IndexService indexService) {

		this.indexConfig = indexConfig;
		this.indexName = indexConfig.getIndexName();
		this.numberOfShards = indexConfig.getNumberOfShards();
		this.indexService = indexService;

		this.documentStorage = documentStorage;

		this.shardPool = Executors.newCachedThreadPool(new ZuliaThreadFactory(indexName + "-shards"));

		this.parsers = new GenericObjectPool<>(new BasePooledObjectFactory<ZuliaMultiFieldQueryParser>() {

			@Override
			public ZuliaMultiFieldQueryParser create() throws Exception {
				return new ZuliaMultiFieldQueryParser(getPerFieldAnalyzer(), ZuliaIndex.this.indexConfig);
			}

			@Override
			public PooledObject<ZuliaMultiFieldQueryParser> wrap(ZuliaMultiFieldQueryParser obj) {
				return new DefaultPooledObject<>(obj);
			}

		});

		this.indexLock = new ReentrantReadWriteLock(true);
		this.shardMap = new ConcurrentHashMap<>();

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

		this.analyzerFactory = new ZuliaAnalyzerFactory(indexConfig);

	}

	public void updateIndexSettings(IndexSettings request) throws Exception {
		indexLock.writeLock().lock();
		try {

			indexService.createIndex(indexConfig.getIndexSettings());
			indexConfig.configure(request);
		}
		finally {
			indexLock.writeLock().unlock();
		}
	}

	public FieldConfig.FieldType getSortFieldType(String fieldName) {
		return indexConfig.getFieldTypeForSortField(fieldName);
	}

	private void doCommit(boolean force) {
		indexLock.readLock().lock();
		try {
			Collection<ZuliaShard> shards = shardMap.values();
			for (ZuliaShard shard : shards) {
				try {
					if (force) {
						shard.forceCommit();
					}
					else {
						shard.doCommit();
					}
				}
				catch (Exception e) {
					LOG.log(Level.SEVERE, "Failed to flush shard <" + shard.getShardNumber() + "> for index <" + indexName + ">", e);
				}
			}
		}
		finally {
			indexLock.readLock().unlock();
		}

	}

	public void unload(boolean terminate) throws IOException {
		indexLock.writeLock().lock();
		try {
			LOG.info("Canceling timers for <" + indexName + ">");
			commitTask.cancel();
			commitTimer.cancel();

			if (!terminate) {
				LOG.info("Committing <" + indexName + ">");
				doCommit(true);
			}

			LOG.info("Shutting shard pool for <" + indexName + ">");
			shardPool.shutdownNow();

			for (Integer shardNumber : shardMap.keySet()) {
				unloadShard(shardNumber, terminate);
			}
		}
		finally {
			indexLock.writeLock().unlock();
		}
	}

	private void loadShard(int shardNumber) throws Exception {
		indexLock.writeLock().lock();
		try {
			if (!shardMap.containsKey(shardNumber)) {

				//Just for clarity
				IndexShardInterface indexShardInterface = this;

				//doesnt need to be done each time and it is done in StartNode but helps with test cases that take different paths

				ZuliaShard s = new ZuliaShard(shardNumber, indexShardInterface, indexConfig, new FacetsConfig());
				shardMap.put(shardNumber, s);

				LOG.info("Loaded shard <" + shardNumber + "> for index <" + indexName + ">");
				LOG.info("Current shards <" + (new TreeSet<>(shardMap.keySet())) + "> for index <" + indexName + ">");

			}
		}
		finally {
			indexLock.writeLock().unlock();
		}
	}

	public IndexWriter getIndexWriter(int shardNumber) throws Exception {

		Directory d = MMapDirectory.open(getPathForIndex(shardNumber));

		IndexWriterConfig config = new IndexWriterConfig(getPerFieldAnalyzer());

		config.setMaxBufferedDocs(Integer.MAX_VALUE);
		config.setRAMBufferSizeMB(100);
		config.setIndexDeletionPolicy(new IndexDeletionPolicy() {
			public void onInit(List<? extends IndexCommit> commits) {
				// Note that commits.size() should normally be 1:
				onCommit(commits);
			}

			/**
			 * Deletes all commits except the most recent one.
			 */
			@Override
			public void onCommit(List<? extends IndexCommit> commits) {
				// Note that commits.size() should normally be 2 (if not
				// called by onInit above):
				int size = commits.size();
				for (int i = 0; i < size - 1; i++) {
					//LOG.info("Deleting old commit for shard <" + shardNumber + "> on index <" + indexName);
					commits.get(i).delete();
				}
			}
		});

		//ConcurrentMergeScheduler concurrentMergeScheduler = new ConcurrentMergeScheduler();
		//concurrentMergeScheduler.setMaxMergesAndThreads(8,2);
		//config.setMergeScheduler(concurrentMergeScheduler);

		config.setUseCompoundFile(false);

		NRTCachingDirectory nrtCachingDirectory = new NRTCachingDirectory(d, 15, 90);

		return new IndexWriter(nrtCachingDirectory, config);
	}

	private Path getPathForIndex(int shardNumber) {
		return Paths.get("indexes", indexName + "_" + shardNumber + "_idx");
	}

	private Path getPathForFacetsIndex(int shardNumber) {
		return Paths.get("indexes", indexName + "_" + shardNumber + "_facets");
	}

	public DirectoryTaxonomyWriter getTaxoWriter(int shardNumber) throws IOException {

		Directory d = MMapDirectory.open(getPathForFacetsIndex(shardNumber));

		NRTCachingDirectory nrtCachingDirectory = new NRTCachingDirectory(d, 2, 10);

		return new DirectoryTaxonomyWriter(nrtCachingDirectory);
	}

	public PerFieldAnalyzerWrapper getPerFieldAnalyzer() throws Exception {
		return analyzerFactory.getPerFieldAnalyzer();
	}

	public void unloadShard(int shardNumber, boolean terminate) throws IOException {
		indexLock.writeLock().lock();
		try {

			if (shardMap.containsKey(shardNumber)) {
				ZuliaShard s = shardMap.remove(shardNumber);
				if (s != null) {
					LOG.info("Closing shard <" + shardNumber + "> for index <" + indexName + ">");
					s.close(terminate);
					LOG.info("Removed shard <" + shardNumber + "> for index <" + indexName + ">");
					LOG.info("Current shards <" + (new TreeSet<>(shardMap.keySet())) + "> for index <" + indexName + ">");
				}
			}

		}
		finally {
			indexLock.writeLock().unlock();
		}

	}

	public void deleteIndex() throws Exception {

		indexService.removeIndex(indexName);

		for (int i = 0; i < numberOfShards; i++) {
			{
				Path p = getPathForIndex(i);
				Files.walkFileTree(p, new DeletingFileVisitor());
			}
			{
				Path p = getPathForFacetsIndex(i);
				Files.walkFileTree(p, new DeletingFileVisitor());
			}

		}

		documentStorage.drop();

	}

	public StoreResponse internalStore(StoreRequest storeRequest) throws Exception {
		indexLock.readLock().lock();

		try {

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

				ZuliaShard s = findShardFromUniqueId(uniqueId);
				s.index(uniqueId, timestamp, document, resultDocument.getMetadataList());

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
		finally {
			indexLock.readLock().unlock();
		}
	}

	private ZuliaShard findShardFromUniqueId(String uniqueId) throws ShardDoesNotExistException {
		int shardNumber = ShardUtil.findShardForUniqueId(uniqueId, numberOfShards);
		ZuliaShard zuliaShard = shardMap.get(shardNumber);
		if (zuliaShard == null) {
			throw new ShardDoesNotExistException(indexName, shardNumber);
		}
		return zuliaShard;
	}

	public DeleteResponse deleteDocument(DeleteRequest deleteRequest) throws Exception {

		indexLock.readLock().lock();

		try {

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
		finally {
			indexLock.readLock().unlock();
		}
	}

	public BooleanQuery handleCosineSimQuery(CosineSimRequest cosineSimRequest) {
		indexLock.readLock().lock();

		try {

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
				booleanQueryBuilder.add(new BooleanClause(new TermQuery(new org.apache.lucene.index.Term(fieldName, signature[i] ? "1" : "0")),
						BooleanClause.Occur.SHOULD));
			}

			return booleanQueryBuilder.build();

		}
		finally {
			indexLock.readLock().unlock();
		}
	}

	public Query getQuery(QueryRequest qr) throws Exception {
		indexLock.readLock().lock();

		try {

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

			if (!qr.getFilterQueryList().isEmpty() || !qr.getCosineSimRequestList().isEmpty() || !facetToValues.isEmpty()) {
				BooleanQuery.Builder booleanQuery = new BooleanQuery.Builder();
				for (ZuliaQuery.Query filterQuery : qr.getFilterQueryList()) {
					Query filterLuceneQuery = parseQueryToLucene(filterQuery);
					booleanQuery.add(filterLuceneQuery, BooleanClause.Occur.FILTER);
				}

				for (Map.Entry<String, Set<String>> entry : facetToValues.entrySet()) {
					String indexFieldName = FacetsConfig.DEFAULT_INDEX_FIELD_NAME + "." + entry.getKey();

					BooleanQuery.Builder facetQuery = new BooleanQuery.Builder();
					for (String value : entry.getValue()) {
						facetQuery.add(new BooleanClause(new TermQuery(new org.apache.lucene.index.Term(indexFieldName, value)), BooleanClause.Occur.SHOULD));
					}

					booleanQuery.add(facetQuery.build(), BooleanClause.Occur.FILTER);
				}

				for (CosineSimRequest cosineSimRequest : qr.getCosineSimRequestList()) {
					BooleanQuery cosineQuery = handleCosineSimQuery(cosineSimRequest);
					booleanQuery.add(cosineQuery, BooleanClause.Occur.MUST);
				}

				booleanQuery.add(q, BooleanClause.Occur.MUST);
				q = booleanQuery.build();
			}

			return q;

		}
		finally {
			indexLock.readLock().unlock();
		}
	}

	private Query parseQueryToLucene(ZuliaQuery.Query zuliaQuery) throws Exception {
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
				qp.setDefaultField(indexConfig.getIndexSettings().getDefaultSearchField());
			}
			else {
				Set<String> fields = new LinkedHashSet<>();

				HashMap<String, Float> boostMap = new HashMap<>();
				for (String queryField : queryFields) {

					if (queryField.contains("^")) {
						try {
							float boost = Float.parseFloat(queryField.substring(queryField.indexOf("^") + 1));
							queryField = queryField.substring(0, queryField.indexOf("^"));
							boostMap.put(queryField, boost);
						}
						catch (Exception e) {
							throw new IllegalArgumentException("Invalid queryText field boost <" + queryField + ">");
						}
					}
					fields.add(queryField);

				}
				qp.setDefaultFields(fields, boostMap);
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

	public IndexShardResponse internalQuery(Query query, final QueryRequest queryRequest) throws Exception {
		indexLock.readLock().lock();
		try {
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
							int docId = sr.getDocId();
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

							after = new FieldDoc(docId, score, sortTerms, sr.getShard());
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

			for (final ZuliaShard shard : shardMap.values()) {

				Future<ShardQueryResponse> response = shardPool.submit(() -> {

					QueryCacheKey queryCacheKey = null;

					if (!queryRequest.getDontCache()) {
						queryCacheKey = new QueryCacheKey(queryRequest);
					}

					return shard
							.queryShard(query, fieldSimilarityMap, requestedAmount, lastScoreDocMap.get(shard.getShardNumber()), queryRequest.getFacetRequest(),
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
		finally {
			indexLock.readLock().unlock();
		}

	}

	public Integer getNumberOfShards() {
		return numberOfShards;
	}

	public double getShardTolerance() {
		return indexConfig.getIndexSettings().getShardTolerance();
	}

	public void reloadIndexSettings() throws Exception {
		indexLock.writeLock().lock();
		try {

			IndexSettings indexSettings = indexService.getIndex(indexName);
			indexConfig.configure(indexSettings);

			parsers.clear();

			//force analyzer to be fetched first so it doesn't fail only on one shard below
			getPerFieldAnalyzer();

			for (ZuliaShard s : shardMap.values()) {
				try {
					s.updateIndexSettings(indexSettings);
				}
				catch (Exception ignored) {
				}
			}

		}
		finally {
			indexLock.writeLock().unlock();
		}
	}

	public OptimizeResponse optimize(OptimizeRequest request) throws Exception {
		indexLock.readLock().lock();
		try {
			int maxNumberOfSegments = 1;
			if (request.getMaxNumberOfSegments() > 0) {
				maxNumberOfSegments = request.getMaxNumberOfSegments();
			}
			for (final ZuliaShard shard : shardMap.values()) {
				shard.optimize(maxNumberOfSegments);
			}

		}
		finally {
			indexLock.readLock().unlock();
		}
		reloadIndexSettings();
		return OptimizeResponse.newBuilder().build();
	}

	public GetNumberOfDocsResponse getNumberOfDocs() throws Exception {
		indexLock.readLock().lock();
		try {
			List<Future<ShardCountResponse>> responses = new ArrayList<>();

			for (final ZuliaShard shard : shardMap.values()) {

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
		finally {
			indexLock.readLock().unlock();
		}
	}

	public GetFieldNamesResponse getFieldNames(GetFieldNamesRequest request) throws Exception {
		indexLock.readLock().lock();
		try {
			List<Future<GetFieldNamesResponse>> responses = new ArrayList<>();

			for (final ZuliaShard shard : shardMap.values()) {

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
		finally {
			indexLock.readLock().unlock();
		}
	}

	public ClearResponse clear(ZuliaServiceOuterClass.ClearRequest request) throws Exception {
		indexLock.writeLock().lock();
		try {
			List<Future<Void>> responses = new ArrayList<>();

			for (final ZuliaShard shard : shardMap.values()) {

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
		finally {
			indexLock.writeLock().unlock();
		}
	}

	public InternalGetTermsResponse getTerms(final GetTermsRequest request) throws Exception {
		indexLock.readLock().lock();
		try {
			List<Future<GetTermsResponse>> responses = new ArrayList<>();

			for (final ZuliaShard shard : shardMap.values()) {

				Future<GetTermsResponse> response = shardPool.submit(() -> shard.getTerms(request));

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
		finally {
			indexLock.readLock().unlock();
		}
	}

	public void storeAssociatedDocument(String uniqueId, String fileName, InputStream is, long clusterTime, HashMap<String, String> metadataMap)
			throws Exception {
		indexLock.readLock().lock();
		try {
			documentStorage.storeAssociatedDocument(uniqueId, fileName, is, clusterTime, metadataMap);
		}
		finally {
			indexLock.readLock().unlock();
		}
	}

	public InputStream getAssociatedDocumentStream(String uniqueId, String fileName) throws IOException {
		indexLock.readLock().lock();
		try {
			return documentStorage.getAssociatedDocumentStream(uniqueId, fileName);
		}
		finally {
			indexLock.readLock().unlock();
		}
	}

	public void getAssociatedDocuments(OutputStream outputStream, Document filter) throws IOException {
		indexLock.readLock().lock();
		try {
			documentStorage.getAssociatedDocuments(outputStream, filter);
		}
		finally {
			indexLock.readLock().unlock();
		}
	}

	private ResultDocument getSourceDocument(String uniqueId, FetchType resultFetchType, List<String> fieldsToReturn, List<String> fieldsToMask)
			throws Exception {
		indexLock.readLock().lock();
		try {
			ZuliaShard s = findShardFromUniqueId(uniqueId);
			return s.getSourceDocument(uniqueId, resultFetchType, fieldsToReturn, fieldsToMask);
		}
		finally {
			indexLock.readLock().unlock();
		}
	}

	public AssociatedDocument getAssociatedDocument(String uniqueId, String fileName, FetchType associatedFetchType) throws Exception {
		indexLock.readLock().lock();
		try {
			return documentStorage.getAssociatedDocument(uniqueId, fileName, associatedFetchType);
		}
		finally {
			indexLock.readLock().unlock();
		}
	}

	public List<AssociatedDocument> getAssociatedDocuments(String uniqueId, FetchType associatedFetchType) throws Exception {
		indexLock.readLock().lock();
		try {
			return documentStorage.getAssociatedDocuments(uniqueId, associatedFetchType);
		}
		finally {
			indexLock.readLock().unlock();
		}
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

	public void loadShards() {
		//TODO load shards
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
}
