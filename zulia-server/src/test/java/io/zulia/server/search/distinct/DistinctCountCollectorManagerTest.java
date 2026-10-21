package io.zulia.server.search.distinct;

import io.zulia.message.ZuliaIndex.FieldConfig.FieldType;
import io.zulia.message.ZuliaIndex.SortAs.StringHandling;
import io.zulia.message.ZuliaQuery.DistinctCountInternal;
import io.zulia.message.ZuliaQuery.DistinctCountRequest;
import io.zulia.server.config.SortFieldInfo;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.StringHelper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Predicate;
import java.util.stream.LongStream;

/**
 * The per segment collection must produce the exact sorted hash set of the matched terms, across segments and slices.
 */
public class DistinctCountCollectorManagerTest {

	private static final String STRING_FIELD = "journal_zss_STRING";
	private static final String LONG_FIELD = "year_zss_NUMERIC_INT";
	private static final int DEFAULT_CAP = DistinctCountCollectorManager.DEFAULT_MAX_GROUPS_PER_SHARD;

	private record Row(int id, String journal, int year, String tag) {
	}

	private static final List<Row> ROWS = new ArrayList<>();
	private static ByteBuffersDirectory directory;
	private static DirectoryReader reader;
	private static ExecutorService executor;

	@BeforeAll
	static void index() throws Exception {
		for (int i = 0; i < 600; i++) {
			// 120 journals, each on about five documents spread over every segment, a rare tag on a handful of documents
			ROWS.add(new Row(i, "JOURNAL" + (i % 120), 2000 + (i % 7), i % 97 == 0 ? "rare" : "common"));
		}
		directory = new ByteBuffersDirectory();
		IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer()).setMergePolicy(NoMergePolicy.INSTANCE);
		try (IndexWriter writer = new IndexWriter(directory, config)) {
			for (Row row : ROWS) {
				Document doc = new Document();
				doc.add(new StringField("tag", row.tag(), Field.Store.NO));
				doc.add(new StringField("half", row.id() % 2 == 0 ? "even" : "odd", Field.Store.NO)); // half of every segment, for the sparse scan
				doc.add(new SortedSetDocValuesField(STRING_FIELD, new BytesRef(row.journal())));
				doc.add(new SortedNumericDocValuesField(LONG_FIELD, row.year()));
				if (row.id() % 2 == 0) {
					doc.add(new SortedNumericDocValuesField(LONG_FIELD, 1999)); // a second value on half the documents
				}
				writer.addDocument(doc);
				if (row.id() % 50 == 49) {
					writer.flush(); // a new segment every 50 documents
				}
			}
		}
		reader = DirectoryReader.open(directory);
		executor = Executors.newVirtualThreadPerTaskExecutor();
		Assertions.assertTrue(reader.leaves().size() >= 10, "expected many segments, got " + reader.leaves().size());
	}

	@AfterAll
	static void close() throws Exception {
		reader.close();
		directory.close();
		executor.close();
	}

	@Test
	public void denseMatchAllAcrossSegmentsIsExact() throws Exception {
		DistinctCountInternal internal = collect(MatchAllDocsQuery.INSTANCE, STRING_FIELD, true, DEFAULT_CAP, executor);
		assertStringKeys(internal, r -> true);
		Assertions.assertEquals(FieldType.STRING, internal.getFieldType());
	}

	@Test
	public void sparseFilterUsesTheSameKeys() throws Exception {
		DistinctCountInternal internal = collect(new TermQuery(new Term("tag", "rare")), STRING_FIELD, true, DEFAULT_CAP, executor);
		assertStringKeys(internal, r -> r.tag().equals("rare"));
		Assertions.assertTrue(internal.getCount() > 1 && internal.getCount() < 20, "rare should hit a handful of journals: " + internal.getCount());
	}

	@Test
	public void anInlineExecutorGivesTheSameResult() throws Exception {
		DistinctCountInternal parallel = collect(MatchAllDocsQuery.INSTANCE, STRING_FIELD, true, DEFAULT_CAP, executor);
		DistinctCountInternal inline = collect(MatchAllDocsQuery.INSTANCE, STRING_FIELD, true, DEFAULT_CAP, Runnable::run);
		Assertions.assertEquals(parallel, inline);
	}

	@Test
	public void stringCapAboveEverySegmentShipsTheExactCountAndTheFirstCapKeys() throws Exception {
		// every segment holds 50 distinct journals, so a cap of 60 is only exceeded by the union across segments
		DistinctCountInternal internal = collect(MatchAllDocsQuery.INSTANCE, STRING_FIELD, true, 60, executor);
		Assertions.assertTrue(internal.getCapped());
		Assertions.assertEquals(120, internal.getCount(), "the shard still knows its exact count");
		DistinctCountInternal full = collect(MatchAllDocsQuery.INSTANCE, STRING_FIELD, true, DEFAULT_CAP, executor);
		Assertions.assertEquals(full.getKeyList().subList(0, 60), internal.getKeyList(), "the sixty smallest hashes ship, in order");

		DistinctCountInternal atCount = collect(MatchAllDocsQuery.INSTANCE, STRING_FIELD, true, 120, executor);
		Assertions.assertFalse(atCount.getCapped(), "a cap equal to the count is not exceeded");
		Assertions.assertEquals(120, atCount.getKeyCount());
	}

	@Test
	public void stringCapBelowOneSegmentStillCountsTheShardExactly() throws Exception {
		// a single segment already exceeds a cap of 30, and the shard still unions every segment to know its own count
		DistinctCountInternal internal = collect(MatchAllDocsQuery.INSTANCE, STRING_FIELD, true, 30, executor);
		Assertions.assertTrue(internal.getCapped());
		Assertions.assertEquals(120, internal.getCount(), "the shard's exact count, not one segment's");
		DistinctCountInternal full = collect(MatchAllDocsQuery.INSTANCE, STRING_FIELD, true, DEFAULT_CAP, executor);
		Assertions.assertEquals(full.getKeyList().subList(0, 30), internal.getKeyList(), "the thirty smallest hashes ship");
	}

	@Test
	public void overTheMemoryGuardTheShardKeepsItsSmallestHashes() throws Exception {
		// 600 set terms over the segments against a guard of 100, so no segment's full hash array is built
		DistinctCountInternal full = collect(MatchAllDocsQuery.INSTANCE, STRING_FIELD, true, DEFAULT_CAP, executor);

		DistinctCountInternal underCap = collect(MatchAllDocsQuery.INSTANCE, STRING_FIELD, true, 120, 100, executor);
		Assertions.assertFalse(underCap.getCapped(), "with every value kept the shard still knows it is not over the cap");
		Assertions.assertEquals(full, underCap);

		DistinctCountInternal overCap = collect(MatchAllDocsQuery.INSTANCE, STRING_FIELD, true, 60, 100, executor);
		Assertions.assertTrue(overCap.getCapped());
		Assertions.assertEquals(61, overCap.getCount(), "the floor is the kept hashes, one past the cap, above any one segment's 50");
		Assertions.assertEquals(full.getKeyList().subList(0, 60), overCap.getKeyList(), "the shard's sixty smallest hashes ship, not one segment's");

		// a cap of 30 is below each segment's 50 values, so each segment goes through the bounded heap
		DistinctCountInternal heap = collect(MatchAllDocsQuery.INSTANCE, STRING_FIELD, true, 30, 100, executor);
		Assertions.assertTrue(heap.getCapped());
		Assertions.assertEquals(50, heap.getCount(), "the largest segment beats the 31 kept hashes as the floor");
		Assertions.assertEquals(full.getKeyList().subList(0, 30), heap.getKeyList());

		DistinctCountInternal sparse = collect(new TermQuery(new Term("tag", "rare")), STRING_FIELD, true, DEFAULT_CAP, 1, executor);
		assertStringKeys(sparse, r -> r.tag().equals("rare"));

		// 25 of each segment's 50 journals takes the seek per ordinal scan, and a cap of 5 sends those 25 through the heap
		DistinctCountInternal sparseHeap = collect(new TermQuery(new Term("half", "even")), STRING_FIELD, true, 5, 1, executor);
		Assertions.assertTrue(sparseHeap.getCapped());
		Assertions.assertEquals(25, sparseHeap.getCount(), "the largest segment beats the 6 kept hashes as the floor");
		long[] evens = ROWS.stream().filter(r -> r.id() % 2 == 0).map(Row::journal).distinct()
				.mapToLong(g -> DistinctCountCollectorManager.hash(new BytesRef(g))).sorted().limit(5).toArray();
		Assertions.assertArrayEquals(evens, sparseHeap.getKeyList().stream().mapToLong(Long::longValue).toArray());
	}

	@Test
	public void fewValuesRepeatedOverManySegmentsStayExactUnderASmallCap() throws Exception {
		// four values in each of twenty segments is 80 set terms for 4 distinct values, which must not read as over a cap of 4
		try (ByteBuffersDirectory dir = new ByteBuffersDirectory()) {
			IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer()).setMergePolicy(NoMergePolicy.INSTANCE);
			try (IndexWriter writer = new IndexWriter(dir, config)) {
				for (int segment = 0; segment < 20; segment++) {
					for (String value : List.of("A", "B", "C", "D")) {
						Document doc = new Document();
						doc.add(new SortedSetDocValuesField(STRING_FIELD, new BytesRef(value)));
						writer.addDocument(doc);
					}
					writer.flush();
				}
			}
			try (DirectoryReader repeated = DirectoryReader.open(dir)) {
				Assertions.assertEquals(20, repeated.leaves().size());
				DistinctCountInternal internal = collect(repeated, MatchAllDocsQuery.INSTANCE, STRING_FIELD, true, 4,
						DistinctCountCollectorManager.MAX_EXACT_SET_TERMS, executor);
				Assertions.assertFalse(internal.getCapped());
				Assertions.assertEquals(4, internal.getCount());
			}
		}
	}

	@Test
	public void longFieldKeysAreTheSmallestHashesOfTheValues() throws Exception {
		DistinctCountInternal internal = collect(MatchAllDocsQuery.INSTANCE, LONG_FIELD, false, DEFAULT_CAP, executor);
		Assertions.assertEquals(FieldType.NUMERIC_INT, internal.getFieldType());
		Assertions.assertFalse(internal.getCapped());
		List<Long> years = LongStream.rangeClosed(1999, 2006).map(DistinctCountCollectorManager::hash).sorted().boxed().toList();
		Assertions.assertEquals(years, internal.getKeyList(), "every value of a multi valued document counts");
		Assertions.assertEquals(8, internal.getCount());

		DistinctCountInternal capped = collect(MatchAllDocsQuery.INSTANCE, LONG_FIELD, false, 3, executor);
		Assertions.assertTrue(capped.getCapped());
		Assertions.assertEquals(4, capped.getCount(), "one past the cap is the floor");
		Assertions.assertEquals(years.subList(0, 3), capped.getKeyList(), "the three smallest hashes ship");
	}

	@Test
	public void theHashMatchesLuceneAndIsStable() {
		// a known term keeps its hash across releases, since shards on different versions must agree on every key
		BytesRef term = new BytesRef("JOURNAL7");
		// the literal is the contract: if Lucene's murmur3 ever changed, comparing against it alone would move both sides and hide the drift
		Assertions.assertEquals(7309986091187619658L, DistinctCountCollectorManager.hash(term));
		Assertions.assertEquals(StringHelper.murmurhash3_x64_128(term)[0], DistinctCountCollectorManager.hash(term));
		Assertions.assertEquals(DistinctCountCollectorManager.hash(new BytesRef("JOURNAL7")),
				DistinctCountCollectorManager.hash(new BytesRef("xxJOURNAL7xx".getBytes(StandardCharsets.UTF_8), 2, 8)), "offset and length are honored");
		// the long hash is pinned the same way, since numeric keys from different nodes must agree too
		Assertions.assertEquals(-5451962507482445012L, DistinctCountCollectorManager.hash(1L));
		Assertions.assertEquals(2508243447376252405L, DistinctCountCollectorManager.hash(2000L));
		Assertions.assertEquals(5431930068122443671L, DistinctCountCollectorManager.hash(-5L));
	}

	@Test
	public void aMissingCapIsRejectedLoudly() {
		DistinctCountRequest unstamped = DistinctCountRequest.newBuilder().setField("f").build();
		IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
				() -> new DistinctCountCollectorManager(unstamped, sortField(STRING_FIELD, true)));
		Assertions.assertTrue(e.getMessage().contains("QueryRequestValidator"), e.getMessage());
	}

	private static DistinctCountInternal collect(Query query, String field, boolean stringField, int cap, Executor ex) throws Exception {
		return collect(query, field, stringField, cap, DistinctCountCollectorManager.MAX_EXACT_SET_TERMS, ex);
	}

	private static DistinctCountInternal collect(Query query, String field, boolean stringField, int cap, long maxExactSetTerms, Executor ex) throws Exception {
		return collect(reader, query, field, stringField, cap, maxExactSetTerms, ex);
	}

	private static DistinctCountInternal collect(DirectoryReader from, Query query, String field, boolean stringField, int cap, long maxExactSetTerms,
			Executor ex) throws Exception {
		DistinctCountRequest request = DistinctCountRequest.newBuilder().setField("f").setMaxGroupsPerShard(cap).build();
		DistinctCountCollectorManager manager = new DistinctCountCollectorManager(request, sortField(field, stringField), maxExactSetTerms);
		IndexSearcher searcher = new IndexSearcher(from, ex);
		DistinctValueCollector collected = searcher.search(query, manager);
		return manager.toInternal(collected, from.leaves(), searcher.getTaskExecutor());
	}

	private static SortFieldInfo sortField(String internalName, boolean string) {
		return new SortFieldInfo(internalName, string ? FieldType.STRING : FieldType.NUMERIC_INT, StringHandling.STANDARD);
	}

	private static void assertStringKeys(DistinctCountInternal internal, Predicate<Row> filter) {
		long[] expected = ROWS.stream().filter(filter).map(Row::journal).distinct().mapToLong(g -> DistinctCountCollectorManager.hash(new BytesRef(g))).sorted()
				.toArray();
		Assertions.assertFalse(internal.getCapped());
		Assertions.assertEquals(expected.length, internal.getCount());
		Assertions.assertArrayEquals(expected, internal.getKeyList().stream().mapToLong(Long::longValue).toArray());
		Assertions.assertArrayEquals(expected, Arrays.stream(expected).sorted().toArray(), "keys ship in ascending order");
	}
}
