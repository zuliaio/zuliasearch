package io.zulia.server.index;

import info.debatty.java.lsh.SuperBit;
import io.zulia.ZuliaConstants;
import io.zulia.message.ZuliaBase.AssociatedDocument;
import io.zulia.message.ZuliaBase.Node;
import io.zulia.message.ZuliaBase.ResultDocument;
import io.zulia.message.ZuliaBase.ShardCountResponse;
import io.zulia.message.ZuliaBase.Similarity;
import io.zulia.message.ZuliaIndex.FieldConfig;
import io.zulia.message.ZuliaIndex.IndexSettings;
import io.zulia.message.ZuliaQuery;
import io.zulia.message.ZuliaQuery.CosineSimRequest;
import io.zulia.message.ZuliaQuery.FetchType;
import io.zulia.message.ZuliaQuery.FieldSimilarity;
import io.zulia.message.ZuliaQuery.HighlightRequest;
import io.zulia.message.ZuliaQuery.IndexShardResponse;
import io.zulia.message.ZuliaQuery.ShardQueryResponse;
import io.zulia.message.ZuliaQuery.SortRequest;
import io.zulia.message.ZuliaServiceOuterClass.DeleteRequest;
import io.zulia.message.ZuliaServiceOuterClass.GetFieldNamesResponse;
import io.zulia.message.ZuliaServiceOuterClass.GetNumberOfDocsResponse;
import io.zulia.message.ZuliaServiceOuterClass.GetTermsRequest;
import io.zulia.message.ZuliaServiceOuterClass.GetTermsResponse;
import io.zulia.message.ZuliaServiceOuterClass.InternalGetTermsResponse;
import io.zulia.message.ZuliaServiceOuterClass.QueryRequest;
import io.zulia.message.ZuliaServiceOuterClass.StoreRequest;
import io.zulia.server.config.IndexService;
import io.zulia.server.config.ServerIndexConfig;
import io.zulia.server.exceptions.ShardDoesNotExist;
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
import org.apache.lucene.search.BoostQuery;
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
import java.util.Collections;
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

	private Map<Node, Set<Integer>> nodeToShardMap;
	private Map<Integer, Node> shardToNodeMap;
	private Timer commitTimer;
	private TimerTask commitTask;
	private ZuliaAnalyzerFactory analyzerFactory;
	private final IndexService indexService;

	private FacetsConfig facetsConfig;

	public ZuliaIndex(ServerIndexConfig indexConfig, DocumentStorage documentStorage, IndexService indexService) throws Exception {

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

	/** From org.apache.solr.search.QueryUtils **/
	public static boolean isNegative(Query q) {
		if (!(q instanceof BooleanQuery))
			return false;
		BooleanQuery bq = (BooleanQuery) q;
		Collection<BooleanClause> clauses = bq.clauses();
		if (clauses.size() == 0)
			return false;
		for (BooleanClause clause : clauses) {
			if (!clause.isProhibited())
				return false;
		}
		return true;
	}

	/** Fixes a negative query by adding a MatchAllDocs query clause.
	 * The query passed in *must* be a negative query.
	 */
	public static Query fixNegativeQuery(Query q) {
		float boost = 1f;
		if (q instanceof BoostQuery) {
			BoostQuery bq = (BoostQuery) q;
			boost = bq.getBoost();
			q = bq.getQuery();
		}
		BooleanQuery bq = (BooleanQuery) q;
		BooleanQuery.Builder newBqB = new BooleanQuery.Builder();
		newBqB.setDisableCoord(bq.isCoordDisabled());
		newBqB.setMinimumNumberShouldMatch(bq.getMinimumNumberShouldMatch());
		for (BooleanClause clause : bq) {
			newBqB.add(clause);
		}
		newBqB.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST);
		BooleanQuery newBq = newBqB.build();
		return new BoostQuery(newBq, boost);
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
				FacetsConfig.DEFAULT_DIM_CONFIG.multiValued = true;
				facetsConfig = new FacetsConfig();

				ZuliaShard s = new ZuliaShard(shardNumber, indexShardInterface, indexConfig, facetsConfig);
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

	private void balance(Set<Node> currentNodes) {
		indexLock.writeLock().lock();
		try {
			boolean balanced = false;
			do {
				int minShardsForNode = Integer.MAX_VALUE;
				int maxShardsForNode = Integer.MIN_VALUE;
				Node minNode = null;
				Node maxNode = null;
				List<Node> shuffledNodes = new ArrayList<>(currentNodes);
				Collections.shuffle(shuffledNodes);
				for (Node m : shuffledNodes) {
					int shardsForNodeCount = 0;
					Set<Integer> shardsForNode = nodeToShardMap.get(m);
					if (shardsForNode != null) {
						shardsForNodeCount = shardsForNode.size();
					}
					if (shardsForNodeCount < minShardsForNode) {
						minShardsForNode = shardsForNodeCount;
						minNode = m;
					}
					if (shardsForNodeCount > maxShardsForNode) {
						maxShardsForNode = shardsForNodeCount;
						maxNode = m;
					}
				}

				if ((maxShardsForNode - minShardsForNode) > 1) {
					moveShard(maxNode, minNode);
				}
				else {
					if ((maxShardsForNode - minShardsForNode == 1)) {
						boolean move = Math.random() >= 0.5;
						if (move) {
							moveShard(maxNode, minNode);
						}
					}

					balanced = true;

				}

			}
			while (!balanced);
		}
		finally {
			indexLock.writeLock().unlock();
		}

	}

	private void moveShard(Node fromNode, Node toNode) {
		int valueToMove = nodeToShardMap.get(fromNode).iterator().next();

		LOG.info("Moving shard <" + valueToMove + "> from <" + fromNode + "> to <" + toNode + "> of Index <" + indexName + ">");
		nodeToShardMap.get(fromNode).remove(valueToMove);

		if (!nodeToShardMap.containsKey(toNode)) {
			nodeToShardMap.put(toNode, new HashSet<>());
		}

		nodeToShardMap.get(toNode).add(valueToMove);
	}

	private void mapSanityCheck(Set<Node> currentNodes) {
		indexLock.writeLock().lock();
		try {
			// add all shards to a set
			Set<Integer> allShards = new HashSet<>();
			for (int shard = 0; shard < indexConfig.getNumberOfShards(); shard++) {
				allShards.add(shard);
			}

			// ensure all members are in the map and contain an empty set
			for (Node node : currentNodes) {
				if (!nodeToShardMap.containsKey(node)) {
					nodeToShardMap.put(node, new HashSet<>());
				}
				if (nodeToShardMap.get(node) == null) {
					nodeToShardMap.put(node, new HashSet<>());
				}
			}

			// get all nodes of the map
			Set<Node> mapNodes = nodeToShardMap.keySet();
			for (Node node : mapNodes) {

				// get current shards
				Set<Integer> shards = nodeToShardMap.get(node);

				Set<Integer> invalidShards = new HashSet<>();
				// check if valid shard
				shards.stream().filter(shard -> !allShards.contains(shard)).forEach(shard -> {
					if ((shard < 0) || (shard >= indexConfig.getNumberOfShards())) {
						LOG.severe("Shard <" + shard + "> should not exist for cluster");
					}
					else {
						LOG.severe("Shard <" + shard + "> is duplicated in node <" + node + ">");
					}
					invalidShards.add(shard);

				});
				// remove any invalid shards for the cluster
				shards.removeAll(invalidShards);
				// remove from all shards to keep track of shards already used
				allShards.removeAll(shards);
			}

			// adds any shards that are missing back to the first node
			if (!allShards.isEmpty()) {
				LOG.severe("Shards <" + allShards + "> are missing from the cluster. Adding back in.");
				nodeToShardMap.values().iterator().next().addAll(allShards);
			}
		}
		finally {
			indexLock.writeLock().unlock();
		}

	}

	public ZuliaShard findShardFromUniqueId(String uniqueId) throws ShardDoesNotExist {
		indexLock.readLock().lock();
		try {
			int shardNumber = getShardNumberForUniqueId(uniqueId);
			ZuliaShard s = shardMap.get(shardNumber);
			if (s == null) {
				throw new ShardDoesNotExist(indexName, shardNumber);
			}
			return s;
		}
		finally {
			indexLock.readLock().unlock();
		}
	}

	public Node findNode(String uniqueId) {
		indexLock.readLock().lock();
		try {
			int shardNumber = getShardNumberForUniqueId(uniqueId);
			return shardToNodeMap.get(shardNumber);
		}
		finally {
			indexLock.readLock().unlock();
		}
	}

	public Map<Integer, Node> getShardToNodeMap() {
		return new HashMap<>(shardToNodeMap);
	}

	private int getShardNumberForUniqueId(String uniqueId) {
		int numShards = indexConfig.getNumberOfShards();
		return ShardUtil.findShardForUniqueId(uniqueId, numShards);
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

	public void storeInternal(StoreRequest storeRequest) throws Exception {
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

		}
		finally {
			indexLock.readLock().unlock();
		}
	}

	/** From org.apache.solr.search.QueryUtils **/

	public void deleteDocument(DeleteRequest deleteRequest) throws Exception {

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

		}
		finally {
			indexLock.readLock().unlock();
		}
	}

	public void handleCosineSimQuery(List<Query> scoredFilterQueries, Map<String, Similarity> similarityOverrideMap, CosineSimRequest cosineSimRequest) {
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
				FieldSimilarity fieldSimilarity = FieldSimilarity.newBuilder().setField(fieldName).setSimilarity(Similarity.CONSTANT).build();
				similarityOverrideMap.putIfAbsent(fieldSimilarity.getField(), fieldSimilarity.getSimilarity());
			}

			BooleanQuery query = booleanQueryBuilder.build();
			scoredFilterQueries.add(query);

		}
		finally {
			indexLock.readLock().unlock();
		}
	}

	public Query getQuery(ZuliaQuery.Query lumongoQuery) throws Exception {
		indexLock.readLock().lock();

		ZuliaQuery.Query.Operator defaultOperator = lumongoQuery.getDefaultOp();
		String queryText = lumongoQuery.getQ();
		Integer minimumShouldMatchNumber = lumongoQuery.getMm();
		List<String> queryFields = lumongoQuery.getQfList();

		try {

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

				if (lumongoQuery.getDismax()) {
					qp.enableDismax(lumongoQuery.getDismaxTie());
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
				boolean negative = isNegative(query);
				if (negative) {
					query = fixNegativeQuery(query);
				}
				return query;

			}
			finally {
				parsers.returnObject(qp);
			}

		}
		finally {
			indexLock.readLock().unlock();
		}
	}

	public IndexShardResponse queryInternal(Query query, final QueryRequest queryRequest) throws Exception {
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

			IndexShardResponse.Builder builder = IndexShardResponse.newBuilder();

			List<Future<ShardQueryResponse>> responses = new ArrayList<>();

			for (final ZuliaShard shard : shardMap.values()) {

				Future<ShardQueryResponse> response = shardPool.submit(() -> {

					QueryCacheKey queryCacheKey = null;

					if (!queryRequest.getDontCache()) {
						queryCacheKey = new QueryCacheKey(queryRequest);
					}

					return shard.queryShard(query, null, requestedAmount, lastScoreDocMap.get(shard.getShardNumber()), queryRequest.getFacetRequest(),
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

	public void optimize() throws Exception {
		indexLock.readLock().lock();
		try {
			for (final ZuliaShard shard : shardMap.values()) {
				shard.optimize();
			}

		}
		finally {
			indexLock.readLock().unlock();
		}
		reloadIndexSettings();
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

	public GetFieldNamesResponse getFieldNames() throws Exception {
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

	public void clear() throws Exception {
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

	//handle forwarding, and locking here like other store requests
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

	public ResultDocument getSourceDocument(String uniqueId, Long timestamp, FetchType resultFetchType, List<String> fieldsToReturn, List<String> fieldsToMask,
			List<HighlightRequest> highlightRequests) throws Exception {
		indexLock.readLock().lock();
		try {
			ZuliaShard s = findShardFromUniqueId(uniqueId);
			return s.getSourceDocument(uniqueId, timestamp, resultFetchType, fieldsToReturn, fieldsToMask);
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

}
