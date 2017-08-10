package io.zulia.server.index;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import info.debatty.java.lsh.SuperBit;
import io.zulia.ZuliaConstants;
import io.zulia.message.ZuliaBase.AssociatedDocument;
import io.zulia.message.ZuliaBase.Node;
import io.zulia.message.ZuliaBase.ResultDocument;
import io.zulia.message.ZuliaBase.ShardCountResponse;
import io.zulia.message.ZuliaIndex.FieldConfig;
import io.zulia.message.ZuliaIndex.IndexSettings;
import io.zulia.message.ZuliaQuery.FetchType;
import io.zulia.message.ZuliaQuery.HighlightRequest;
import io.zulia.message.ZuliaServiceOuterClass.DeleteRequest;
import io.zulia.message.ZuliaServiceOuterClass.GetFieldNamesResponse;
import io.zulia.message.ZuliaServiceOuterClass.GetNumberOfDocsResponse;
import io.zulia.message.ZuliaServiceOuterClass.GetTermsRequest;
import io.zulia.message.ZuliaServiceOuterClass.GetTermsResponse;
import io.zulia.message.ZuliaServiceOuterClass.InternalGetTermsResponse;
import io.zulia.message.ZuliaServiceOuterClass.StoreRequest;
import io.zulia.server.config.IndexService;
import io.zulia.server.config.ServerIndexConfig;
import io.zulia.server.exceptions.ShardDoesNotExist;
import io.zulia.server.filestorage.DocumentStorage;
import io.zulia.server.search.ZuliaMultiFieldQueryParser;
import io.zulia.server.util.DeletingFileVisitor;
import io.zulia.util.ZuliaThreadFactory;
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

	public static final String CONFIG_SUFFIX = "_config";

	private static final String STORAGE_DB_SUFFIX = "_rs";

	private static final String RESULT_STORAGE_COLLECTION = "resultStorage";

	private final static Logger log = Logger.getLogger(ZuliaIndex.class.getSimpleName());
	private static final String SETTINGS_ID = "settings";

	private final ServerIndexConfig indexConfig;

	private final GenericObjectPool<ZuliaMultiFieldQueryParser> parsers;
	private final ConcurrentHashMap<Integer, ZuliaShard> segmentMap;
	private final ReadWriteLock indexLock;
	private final ExecutorService segmentPool;
	private final int numberOfShards;
	private final String indexName;

	private final DocumentStorage documentStorage;

	private Map<Node, Set<Integer>> memberToSegmentMap;
	private Map<Integer, Node> segmentToMemberMap;
	private Timer commitTimer;
	private TimerTask commitTask;
	private ZuliaAnalyzerFactory analyzerFactory;
	private final IndexService indexService;

	private LockHandler documentLockHandler;
	private FacetsConfig facetsConfig;

	public ZuliaIndex(ServerIndexConfig indexConfig, DocumentStorage documentStorage, IndexService indexService) throws Exception {

		this.documentLockHandler = new LockHandler();

		this.indexConfig = indexConfig;
		this.indexName = indexConfig.getIndexName();
		this.numberOfShards = indexConfig.getNumberOfShards();
		this.indexService = indexService;

		this.documentStorage = documentStorage;

		this.segmentPool = Executors.newCachedThreadPool(new ZuliaThreadFactory(indexName + "-shards"));

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
		this.segmentMap = new ConcurrentHashMap<>();

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

	public void updateIndexSettings(IndexSettings request) {
		indexLock.writeLock().lock();
		try {

			indexConfig.configure(request);
			storeIndexSettings();
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
			Collection<ZuliaShard> segments = segmentMap.values();
			for (ZuliaShard segment : segments) {
				try {
					if (force) {
						segment.forceCommit();
					}
					else {
						segment.doCommit();
					}
				}
				catch (Exception e) {
					log.log(Level.SEVERE, "Failed to flush shard <" + segment.getShardNumber() + "> for index <" + indexName + ">", e);
				}
			}
		}
		finally {
			indexLock.readLock().unlock();
		}

	}

	public void updateSegmentMap(Map<Node, Set<Integer>> newMemberToSegmentMap) {
		indexLock.writeLock().lock();
		try {
			log.info("Updating segments map");

			this.memberToSegmentMap = newMemberToSegmentMap;
			this.segmentToMemberMap = new HashMap<>();

			for (Node m : memberToSegmentMap.keySet()) {
				for (int i : memberToSegmentMap.get(m)) {
					segmentToMemberMap.put(i, m);
				}
			}

			Member self = hazelcastManager.getSelf();

			Set<Integer> newSegments = memberToSegmentMap.get(self);

			log.info("Settings segments for this node <" + self + "> to <" + newSegments + ">");

			segmentMap.keySet().stream().filter(segmentNumber -> !newSegments.contains(segmentNumber)).forEach(segmentNumber -> {
				try {
					unloadSegment(segmentNumber, false);
				}
				catch (Exception e) {
					log.error("Error unloading segment <" + segmentNumber + "> for index <" + indexName + ">");
					log.error(e.getClass().getSimpleName() + ": ", e);
				}
			});

			newSegments.stream().filter(segmentNumber -> !segmentMap.containsKey(segmentNumber)).forEach(segmentNumber -> {
				try {
					loadSegment(segmentNumber);
				}
				catch (Exception e) {
					log.error("Error loading segment <" + segmentNumber + "> for index <" + indexName + ">");
					log.error(e.getClass().getSimpleName() + ": ", e);
				}
			});

		}
		finally {
			indexLock.writeLock().unlock();
		}

	}

	public void loadAllSegments() throws Exception {
		indexLock.writeLock().lock();
		try {
			Member self = hazelcastManager.getSelf();
			this.memberToSegmentMap = new HashMap<>();
			this.memberToSegmentMap.put(self, new HashSet<>());
			for (int segmentNumber = 0; segmentNumber < numberOfShards; segmentNumber++) {
				loadSegment(segmentNumber);
				this.memberToSegmentMap.get(self).add(segmentNumber);
			}

			this.segmentToMemberMap = new HashMap<>();

			for (Member m : memberToSegmentMap.keySet()) {
				for (int i : memberToSegmentMap.get(m)) {
					segmentToMemberMap.put(i, m);
				}
			}
		}
		finally {
			indexLock.writeLock().unlock();
		}
	}

	public void unload(boolean terminate) throws IOException {
		indexLock.writeLock().lock();
		try {
			log.info("Canceling timers for <" + indexName + ">");
			commitTask.cancel();
			commitTimer.cancel();

			if (!terminate) {
				log.info("Committing <" + indexName + ">");
				doCommit(true);
			}

			log.info("Shutting segment pool for <" + indexName + ">");
			segmentPool.shutdownNow();

			for (Integer segmentNumber : segmentMap.keySet()) {
				unloadSegment(segmentNumber, terminate);
			}
		}
		finally {
			indexLock.writeLock().unlock();
		}
	}

	private void loadSegment(int segmentNumber) throws Exception {
		indexLock.writeLock().lock();
		try {
			if (!segmentMap.containsKey(segmentNumber)) {
				String lockName = indexName + "-" + segmentNumber;
				ILock hzLock = hazelcastManager.getLock(lockName);
				hazelLockMap.put(segmentNumber, hzLock);
				log.info("Waiting for lock for index <" + indexName + "> segment <" + segmentNumber + ">");
				hzLock.lock();
				log.info("Obtained lock for index <" + indexName + "> segment <" + segmentNumber + ">");

				//Just for clarity
				IndexShardInterface indexShardInterface = this;

				//doesnt need to be done each time and it is done in StartNode but helps with test cases that take different paths
				FacetsConfig.DEFAULT_DIM_CONFIG.multiValued = true;
				facetsConfig = new FacetsConfig();

				ZuliaShard s = new ZuliaShard(segmentNumber, indexShardInterface, indexConfig, facetsConfig);
				segmentMap.put(segmentNumber, s);

				log.info("Loaded segment <" + segmentNumber + "> for index <" + indexName + ">");
				log.info("Current segments <" + (new TreeSet<>(segmentMap.keySet())) + "> for index <" + indexName + ">");

			}
		}
		finally {
			indexLock.writeLock().unlock();
		}
	}

	public IndexWriter getIndexWriter(int segmentNumber) throws Exception {

		Directory d = MMapDirectory.open(getPathForIndex(segmentNumber));

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
					//log.info("Deleting old commit for segment <" + segmentNumber + "> on index <" + indexName);
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

	private Path getPathForIndex(int segmentNumber) {
		return Paths.get("indexes", indexName + "_" + segmentNumber + "_idx");
	}

	private Path getPathForFacetsIndex(int segmentNumber) {
		return Paths.get("indexes", indexName + "_" + segmentNumber + "_facets");
	}

	public DirectoryTaxonomyWriter getTaxoWriter(int segmentNumber) throws IOException {

		Directory d = MMapDirectory.open(getPathForFacetsIndex(segmentNumber));

		NRTCachingDirectory nrtCachingDirectory = new NRTCachingDirectory(d, 2, 10);

		return new DirectoryTaxonomyWriter(nrtCachingDirectory);
	}

	public PerFieldAnalyzerWrapper getPerFieldAnalyzer() throws Exception {
		return analyzerFactory.getPerFieldAnalyzer();
	}

	public void unloadSegment(int segmentNumber, boolean terminate) throws IOException {
		indexLock.writeLock().lock();
		try {
			ILock hzLock = hazelLockMap.get(segmentNumber);
			try {
				if (segmentMap.containsKey(segmentNumber)) {
					ZuliaShard s = segmentMap.remove(segmentNumber);
					if (s != null) {
						log.info("Closing segment <" + segmentNumber + "> for index <" + indexName + ">");
						s.close(terminate);
						log.info("Removed segment <" + segmentNumber + "> for index <" + indexName + ">");
						log.info("Current segments <" + (new TreeSet<>(segmentMap.keySet())) + "> for index <" + indexName + ">");
					}
				}

			}
			finally {
				try {
					hzLock.forceUnlock();
					log.info("Unlocked lock for index <" + indexName + "> segment <" + segmentNumber + ">");
				}
				catch (Exception e) {
					log.error("Failed to unlock <" + segmentNumber + ">: ", e);
				}
			}
		}
		finally {
			indexLock.writeLock().unlock();
		}

	}

	/**
	 * called on older cluster node when a new member is added
	 *
	 * @param currentMembers
	 *            - current cluster members
	 * @param memberAdded
	 *            - member that is being added
	 */
	public void handleServerAdded(Set<Member> currentMembers, Member memberAdded) {
		indexLock.writeLock().lock();
		try {
			forceBalance(currentMembers);
		}
		finally {
			indexLock.writeLock().unlock();
		}

	}

	public void forceBalance(Set<Member> currentMembers) {
		indexLock.writeLock().lock();
		try {
			mapSanityCheck(currentMembers);
			balance(currentMembers);

			IExecutorService executorService = hazelcastManager.getExecutorService();

			List<Future<Void>> results = new ArrayList<>();

			for (Member m : currentMembers) {

				try {
					UpdateSegmentsTask ust = new UpdateSegmentsTask(m.getSocketAddress().getPort(), indexName, memberToSegmentMap);
					if (!m.localMember()) {
						Future<Void> dt = executorService.submitToMember(ust, m);
						results.add(dt);
					}
				}
				catch (Exception e) {
					log.error(e.getClass().getSimpleName() + ": ", e);
				}

			}

			try {
				UpdateSegmentsTask ust = new UpdateSegmentsTask(hazelcastManager.getHazelcastPort(), indexName, memberToSegmentMap);
				ust.call();
			}
			catch (Exception e) {
				log.error(e.getClass().getSimpleName() + ": ", e);
			}
			for (Future<Void> result : results) {
				try {
					result.get();
				}
				catch (Exception e) {
					log.error(e.getClass().getSimpleName() + ": ", e);
				}
			}
		}
		finally {
			indexLock.writeLock().unlock();
		}

	}

	/**
	 * Called on older cluster node when member is removed
	 *
	 * @param currentMembers
	 *            - current cluster members
	 * @param memberRemoved
	 *            - member that is being removed
	 */
	public void handleServerRemoved(Set<Member> currentMembers, Member memberRemoved) {
		indexLock.writeLock().lock();
		try {
			Set<Integer> segmentsToRedist = memberToSegmentMap.remove(memberRemoved);
			if (segmentsToRedist != null) {
				Member first = currentMembers.iterator().next();
				memberToSegmentMap.get(first).addAll(segmentsToRedist);
			}

			forceBalance(currentMembers);
		}
		finally {
			indexLock.writeLock().unlock();
		}

	}

	private void balance(Set<Member> currentMembers) {
		indexLock.writeLock().lock();
		try {
			boolean balanced = false;
			do {
				int minSegmentsForMember = Integer.MAX_VALUE;
				int maxSegmentsForMember = Integer.MIN_VALUE;
				Member minMember = null;
				Member maxMember = null;
				List<Member> shuffledMembers = new ArrayList<>(currentMembers);
				Collections.shuffle(shuffledMembers);
				for (Member m : shuffledMembers) {
					int segmentsForMemberCount = 0;
					Set<Integer> segmentsForMember = memberToSegmentMap.get(m);
					if (segmentsForMember != null) {
						segmentsForMemberCount = segmentsForMember.size();
					}
					if (segmentsForMemberCount < minSegmentsForMember) {
						minSegmentsForMember = segmentsForMemberCount;
						minMember = m;
					}
					if (segmentsForMemberCount > maxSegmentsForMember) {
						maxSegmentsForMember = segmentsForMemberCount;
						maxMember = m;
					}
				}

				if ((maxSegmentsForMember - minSegmentsForMember) > 1) {
					moveSegment(maxMember, minMember);
				}
				else {
					if ((maxSegmentsForMember - minSegmentsForMember == 1)) {
						boolean move = Math.random() >= 0.5;
						if (move) {
							moveSegment(maxMember, minMember);
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

	private void moveSegment(Member fromMember, Member toMember) {
		int valueToMove = memberToSegmentMap.get(fromMember).iterator().next();

		log.info("Moving segment <" + valueToMove + "> from <" + fromMember + "> to <" + toMember + "> of Index <" + indexName + ">");
		memberToSegmentMap.get(fromMember).remove(valueToMove);

		if (!memberToSegmentMap.containsKey(toMember)) {
			memberToSegmentMap.put(toMember, new HashSet<>());
		}

		memberToSegmentMap.get(toMember).add(valueToMove);
	}

	private void mapSanityCheck(Set<Member> currentMembers) {
		indexLock.writeLock().lock();
		try {
			// add all segments to a set
			Set<Integer> allSegments = new HashSet<>();
			for (int segment = 0; segment < indexConfig.getNumberOfSegments(); segment++) {
				allSegments.add(segment);
			}

			// ensure all members are in the map and contain an empty set
			for (Member m : currentMembers) {
				if (!memberToSegmentMap.containsKey(m)) {
					memberToSegmentMap.put(m, new HashSet<>());
				}
				if (memberToSegmentMap.get(m) == null) {
					memberToSegmentMap.put(m, new HashSet<>());
				}
			}

			// get all members of the map
			Set<Member> mapMembers = memberToSegmentMap.keySet();
			for (Member m : mapMembers) {

				// get current segments
				Set<Integer> segments = memberToSegmentMap.get(m);

				Set<Integer> invalidSegments = new HashSet<>();
				// check if valid segment
				segments.stream().filter(segment -> !allSegments.contains(segment)).forEach(segment -> {
					if ((segment < 0) || (segment >= indexConfig.getNumberOfSegments())) {
						log.error("Segment <" + segment + "> should not exist for cluster");
					}
					else {
						log.error("Segment <" + segment + "> is duplicated in node <" + m + ">");
					}
					invalidSegments.add(segment);

				});
				// remove any invalid segments for the cluster
				segments.removeAll(invalidSegments);
				// remove from all segments to keep track of segments already used
				allSegments.removeAll(segments);
			}

			// adds any segments that are missing back to the first node
			if (!allSegments.isEmpty()) {
				log.error("Segments <" + allSegments + "> are missing from the cluster. Adding back in.");
				memberToSegmentMap.values().iterator().next().addAll(allSegments);
			}
		}
		finally {
			indexLock.writeLock().unlock();
		}

	}

	public ZuliaShard findShardFromUniqueId(String uniqueId) throws ShardDoesNotExist {
		indexLock.readLock().lock();
		try {
			int shardNumber = getSegmentNumberForUniqueId(uniqueId);
			ZuliaShard s = segmentMap.get(shardNumber);
			if (s == null) {
				throw new ShardDoesNotExist(indexName, shardNumber);
			}
			return s;
		}
		finally {
			indexLock.readLock().unlock();
		}
	}

	public Member findMember(String uniqueId) {
		indexLock.readLock().lock();
		try {
			int segmentNumber = getSegmentNumberForUniqueId(uniqueId);
			return segmentToMemberMap.get(segmentNumber);
		}
		finally {
			indexLock.readLock().unlock();
		}
	}

	public Map<Integer, Member> getSegmentToMemberMap() {
		return new HashMap<>(segmentToMemberMap);
	}

	private int getSegmentNumberForUniqueId(String uniqueId) {
		int numSegments = indexConfig.getNumberOfSegments();
		return SegmentUtil.findSegmentForUniqueId(uniqueId, numSegments);
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

			long timestamp = hazelcastManager.getClusterTime();

			String uniqueId = storeRequest.getUniqueId();
			ReadWriteLock documentLock = documentLockHandler.getLock(uniqueId);
			try {
				documentLock.writeLock().lock();

				if (storeRequest.hasResultDocument()) {
					ResultDocument resultDocument = storeRequest.getResultDocument();
					Document document;
					if (resultDocument.hasDocument()) {
						document = LumongoUtil.byteArrayToMongoDocument(resultDocument.getDocument().toByteArray());
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
				documentLock.writeLock().unlock();
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

			ReadWriteLock documentLock = documentLockHandler.getLock(uniqueId);

			try {
				documentLock.writeLock().lock();

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
				documentLock.writeLock().unlock();
			}

		}
		finally {
			indexLock.readLock().unlock();
		}
	}

	public void handleCosineSimQuery(QueryWithFilters queryWithFilters, Lumongo.CosineSimRequest cosineSimRequest) {
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
				queryWithFilters.addSimilarityOverride(Lumongo.FieldSimilarity.newBuilder().setField(fieldName).setSimilarity(Similarity.CONSTANT).build());
			}

			BooleanQuery query = booleanQueryBuilder.build();
			queryWithFilters.addScoredFilterQuery(query);

		}
		finally {
			indexLock.readLock().unlock();
		}
	}

	public Query getQuery(Lumongo.Query lumongoQuery) throws Exception {
		indexLock.readLock().lock();

		Lumongo.Query.Operator defaultOperator = lumongoQuery.getDefaultOp();
		String queryText = lumongoQuery.getQ();
		Integer minimumShouldMatchNumber = lumongoQuery.getMm();
		List<String> queryFields = lumongoQuery.getQfList();

		try {

			QueryParser.Operator operator = null;
			if (defaultOperator.equals(Lumongo.Query.Operator.OR)) {
				operator = QueryParser.Operator.OR;
			}
			else if (defaultOperator.equals(Lumongo.Query.Operator.AND)) {
				operator = QueryParser.Operator.AND;
			}
			else {
				//this should never happen
				log.error("Unknown operator type: <" + defaultOperator + ">");
			}

			LumongoMultiFieldQueryParser qp = null;
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

	public IndexSegmentResponse queryInternal(final QueryWithFilters queryWithFilters, final QueryRequest queryRequest) throws Exception {
		indexLock.readLock().lock();
		try {
			int amount = queryRequest.getAmount() + queryRequest.getStart();

			if (indexConfig.getNumberOfSegments() != 1) {
				if (!queryRequest.getFetchFull() && (amount > 0)) {
					amount = (int) (((amount / numberOfShards) + indexConfig.getIndexSettings().getMinSegmentRequest()) * indexConfig.getIndexSettings()
							.getRequestFactor());
				}
			}

			final int requestedAmount = amount;

			final HashMap<Integer, FieldDoc> lastScoreDocMap = new HashMap<>();
			FieldDoc after;

			LastResult lr = queryRequest.getLastResult();
			if (lr != null) {
				for (LastIndexResult lir : lr.getLastIndexResultList()) {
					if (indexName.equals(lir.getIndexName())) {
						for (ScoredResult sr : lir.getLastForSegmentList()) {
							int docId = sr.getDocId();
							float score = sr.getScore();

							SortRequest sortRequest = queryRequest.getSortRequest();

							Object[] sortTerms = new Object[sortRequest.getFieldSortCount()];

							int sortTermsIndex = 0;

							SortValues sortValues = sr.getSortValues();
							for (FieldSort fs : sortRequest.getFieldSortList()) {

								String sortField = fs.getSortField();
								FieldConfig.FieldType sortType = indexConfig.getFieldTypeForSortField(sortField);
								if (sortType == null) {
									throw new Exception(sortField + " is not defined as a sortable field");
								}

								SortValue sortValue = sortValues.getSortValue(sortTermsIndex);

								if (sortValue.getExists()) {
									if (IndexConfigUtil.isNumericOrDateFieldType(sortType)) {
										if (IndexConfigUtil.isNumericIntFieldType(sortType)) {
											sortTerms[sortTermsIndex] = sortValue.getIntegerValue();
										}
										else if (IndexConfigUtil.isNumericLongFieldType(sortType)) {
											sortTerms[sortTermsIndex] = sortValue.getLongValue();
										}
										else if (IndexConfigUtil.isNumericFloatFieldType(sortType)) {
											sortTerms[sortTermsIndex] = sortValue.getFloatValue();
										}
										else if (IndexConfigUtil.isNumericDoubleFieldType(sortType)) {
											sortTerms[sortTermsIndex] = sortValue.getDoubleValue();
										}
										else if (IndexConfigUtil.isDateFieldType(sortType)) {
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

							after = new FieldDoc(docId, score, sortTerms, sr.getSegment());
							lastScoreDocMap.put(sr.getSegment(), after);
						}
					}
				}
			}

			IndexSegmentResponse.Builder builder = IndexSegmentResponse.newBuilder();

			List<Future<SegmentResponse>> responses = new ArrayList<>();

			for (final ZuliaShard segment : segmentMap.values()) {

				Future<SegmentResponse> response = segmentPool.submit(() -> {

					QueryCacheKey queryCacheKey = null;

					if (!queryRequest.getDontCache()) {
						queryCacheKey = new QueryCacheKey(queryRequest);
					}

					return segment
							.querySegment(queryWithFilters, requestedAmount, lastScoreDocMap.get(segment.getSegmentNumber()), queryRequest.getFacetRequest(),
									queryRequest.getSortRequest(), queryCacheKey, queryRequest.getResultFetchType(), queryRequest.getDocumentFieldsList(),
									queryRequest.getDocumentMaskedFieldsList(), queryRequest.getHighlightRequestList(), queryRequest.getAnalysisRequestList(),
									queryRequest.getDebug());
				});

				responses.add(response);

			}

			for (Future<SegmentResponse> response : responses) {
				try {
					SegmentResponse rs = response.get();
					builder.addSegmentReponse(rs);
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

	public double getSegmentTolerance() {
		return indexConfig.getIndexSettings().getSegmentTolerance();
	}

	private void storeIndexSettings() {
		indexLock.writeLock().lock();
		try {
			MongoDatabase db = mongo.getDatabase(mongoConfig.getDatabaseName());
			MongoCollection<Document> dbCollection = db.getCollection(indexConfig.getIndexName() + CONFIG_SUFFIX);
			Document settings = IndexConfigUtil.toDocument(indexConfig);
			settings.put(MongoConstants.StandardFields._ID, SETTINGS_ID);

			Document query = new Document();
			query.put(MongoConstants.StandardFields._ID, SETTINGS_ID);

			dbCollection.replaceOne(query, settings, new UpdateOptions().upsert(true));
		}
		finally {
			indexLock.writeLock().unlock();
		}

	}

	public void reloadIndexSettings() throws Exception {
		indexLock.writeLock().lock();
		try {

			IndexSettings indexSettings = indexService.getIndex(indexName);
			indexConfig.configure(indexSettings);

			parsers.clear();

			//force analyzer to be fetched first so it doesn't fail only on one segment below
			getPerFieldAnalyzer();

			for (ZuliaShard s : segmentMap.values()) {
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
			for (final ZuliaShard segment : segmentMap.values()) {
				segment.optimize();
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

			for (final ZuliaShard segment : segmentMap.values()) {

				Future<ShardCountResponse> response = segmentPool.submit(segment::getNumberOfDocs);

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
					throw new Exception("Interrupted while waiting for segment results");
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

			for (final ZuliaShard segment : segmentMap.values()) {

				Future<GetFieldNamesResponse> response = segmentPool.submit(segment::getFieldNames);

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

			for (final ZuliaShard segment : segmentMap.values()) {

				Future<Void> response = segmentPool.submit(() -> {
					segment.clear();
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

			for (final ZuliaShard segment : segmentMap.values()) {

				Future<GetTermsResponse> response = segmentPool.submit(() -> segment.getTerms(request));

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
