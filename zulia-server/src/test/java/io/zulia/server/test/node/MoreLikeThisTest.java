package io.zulia.server.test.node;

import com.google.common.primitives.Floats;
import io.zulia.DefaultAnalyzers;
import io.zulia.client.command.Store;
import io.zulia.client.command.builder.FilterQuery;
import io.zulia.client.command.builder.MoreLikeThisQuery;
import io.zulia.client.command.builder.Search;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.CompleteResult;
import io.zulia.client.result.SearchResult;
import io.zulia.doc.ResultDocBuilder;
import io.zulia.fields.FieldConfigBuilder;
import io.zulia.server.test.node.shared.NodeExtension;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class MoreLikeThisTest {

	@RegisterExtension
	static final NodeExtension nodeExtension = new NodeExtension(3);

	public static final String MLT_TEST = "mltTest";
	public static final String MLT_TEST_SECONDARY = "mltTestSecondary";

	// Documents designed to have known text overlap:
	// doc1-doc4: Java-related (share terms "java", "programming", "object", etc.)
	// doc5-doc6: Python-related (share terms "python", "scripting", "language")
	// doc7: JavaScript (shares some terms with Java docs)
	// doc8-doc10: Machine learning (share terms "machine", "learning", "data")

	@Test
	@Order(1)
	public void createIndex() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("content");
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("title").indexAs(DefaultAnalyzers.STANDARD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("content").indexAs(DefaultAnalyzers.STANDARD));
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("category").indexAs(DefaultAnalyzers.LC_KEYWORD).facet().sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createUnitVector("embedding").index());
		indexConfig.setIndexName(MLT_TEST);
		indexConfig.setNumberOfShards(1);
		indexConfig.setShardCommitInterval(20);

		zuliaWorkPool.createIndex(indexConfig);

		ClientIndexConfig secondary = new ClientIndexConfig();
		secondary.addDefaultSearchField("content");
		secondary.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		secondary.addFieldConfig(FieldConfigBuilder.createString("title").indexAs(DefaultAnalyzers.STANDARD).sort());
		secondary.addFieldConfig(FieldConfigBuilder.createString("content").indexAs(DefaultAnalyzers.STANDARD));
		secondary.addFieldConfig(FieldConfigBuilder.createString("category").indexAs(DefaultAnalyzers.LC_KEYWORD).facet().sort());
		secondary.addFieldConfig(FieldConfigBuilder.createUnitVector("embedding").index());
		secondary.setIndexName(MLT_TEST_SECONDARY);
		secondary.setNumberOfShards(1);
		secondary.setShardCommitInterval(20);
		zuliaWorkPool.createIndex(secondary);
	}

	@Test
	@Order(2)
	public void index() throws Exception {
		// Java-related documents
		indexRecord("1", "Java Programming", "java programming language object oriented design patterns inheritance polymorphism", "java",
				new float[] { 0.9f, 0.1f, 0.0f, 0.0f });
		indexRecord("2", "Java Development",
				"java development software engineering object oriented design best practices enterprise application development programming", "java",
				new float[] { 0.85f, 0.15f, 0.0f, 0.0f });
		indexRecord("3", "Java Virtual Machine", "java virtual machine bytecode compilation runtime environment programming language performance", "java",
				new float[] { 0.8f, 0.1f, 0.1f, 0.0f });
		indexRecord("4", "Java Spring Framework",
				"java spring framework enterprise application development dependency injection inversion of control programming design patterns", "java",
				new float[] { 0.82f, 0.18f, 0.0f, 0.0f });

		// Python-related documents
		indexRecord("5", "Python Scripting", "python scripting language dynamic typing interpreted programming easy to learn beginner friendly", "python",
				new float[] { 0.1f, 0.9f, 0.0f, 0.0f });
		indexRecord("6", "Python Data Science",
				"python data science machine learning pandas numpy statistical analysis data visualization programming language", "python",
				new float[] { 0.1f, 0.7f, 0.2f, 0.0f });

		// JavaScript document
		indexRecord("7", "JavaScript Web Development",
				"javascript web browser frontend development programming language dynamic typing event driven asynchronous", "javascript",
				new float[] { 0.4f, 0.4f, 0.2f, 0.0f });

		// Machine learning documents
		indexRecord("8", "Machine Learning Fundamentals",
				"machine learning artificial intelligence neural networks deep learning training data models prediction classification", "ml",
				new float[] { 0.0f, 0.2f, 0.8f, 0.0f });
		indexRecord("9", "Deep Learning", "deep learning neural networks convolutional recurrent transformer architecture machine learning training data", "ml",
				new float[] { 0.0f, 0.1f, 0.9f, 0.0f });
		indexRecord("10", "Natural Language Processing",
				"natural language processing text analysis sentiment classification machine learning deep learning word embeddings", "ml",
				new float[] { 0.0f, 0.15f, 0.75f, 0.1f });

		// Additional Java doc without vector
		indexRecord("11", "Java Collections", "java collections framework list set map hashmap arraylist linked list data structures programming", "java",
				null);

		// Duplicate content to boost term frequencies
		indexRecord("12", "Advanced Java",
				"java programming advanced topics concurrency multithreading object oriented design patterns factory singleton observer", "java",
				new float[] { 0.88f, 0.12f, 0.0f, 0.0f });

		// Secondary index: more Java-related docs (different IDs) for cross-index MLT
		indexRecord(MLT_TEST_SECONDARY, "100", "Kotlin and Java",
				"kotlin java jvm interoperability programming object oriented design patterns null safety", "kotlin",
				new float[] { 0.7f, 0.2f, 0.1f, 0.0f });
		indexRecord(MLT_TEST_SECONDARY, "101", "Scala on JVM",
				"scala functional programming jvm object oriented type system pattern matching", "scala", new float[] { 0.5f, 0.3f, 0.2f, 0.0f });
		indexRecord(MLT_TEST_SECONDARY, "102", "Pure Python", "python scripting language list comprehension generator decorator", "python",
				new float[] { 0.0f, 0.95f, 0.05f, 0.0f });
	}

	private void indexRecord(String id, String title, String content, String category, float[] embedding) throws Exception {
		indexRecord(MLT_TEST, id, title, content, category, embedding);
	}

	private void indexRecord(String indexName, String id, String title, String content, String category, float[] embedding) throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		Document mongoDocument = new Document();
		mongoDocument.put("id", id);
		mongoDocument.put("title", title);
		mongoDocument.put("content", content);
		mongoDocument.put("category", category);
		mongoDocument.put("embedding", embedding != null ? Floats.asList(embedding) : null);

		Store s = new Store(id, indexName);
		ResultDocBuilder resultDocumentBuilder = ResultDocBuilder.newBuilder().setDocument(mongoDocument);
		s.setResultDocument(resultDocumentBuilder);
		zuliaWorkPool.store(s);
	}

	private static Set<String> resultCategories(SearchResult searchResult) {
		return resultCategories(searchResult, Long.MAX_VALUE);
	}

	private static Set<String> resultCategories(SearchResult searchResult, long topN) {
		return searchResult.getCompleteResults().stream().limit(topN).map(r -> r.getDocument().getString("category")).collect(Collectors.toSet());
	}

	private static Set<String> resultIndexes(SearchResult searchResult) {
		return searchResult.getCompleteResults().stream().map(CompleteResult::getIndexName).collect(Collectors.toSet());
	}

	@Test
	@Order(3)
	public void lexicalMLTFromText() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// MLT from raw text about Java, so it should find Java-related docs.
		// Disable the maxDocFreqPct guard: on this tiny index the discriminating Java terms appear in >25% of docs.
		Search search = new Search(MLT_TEST).setRealtime(true);
		search.addQuery(MoreLikeThisQuery.forText(List.of("content"), "java programming object oriented development design patterns").setMinDocFreq(1)
				.setMinTermFreq(1).setMaxDocFreqPct(100));
		search.setAmount(12);
		SearchResult searchResult = zuliaWorkPool.search(search);

		Assertions.assertTrue(searchResult.getTotalHits() > 0, "MLT from text should return results");

		// Top 4 must be Java docs. Docs 3 and 11 share only two terms with the like text, so their rank vs. the JavaScript doc is not deterministic
		Assertions.assertEquals(Set.of("java"), resultCategories(searchResult, 4),
				"Top MLT results from Java text should be Java docs, got: " + searchResult.getUniqueIds());
	}

	@Test
	@Order(4)
	public void lexicalMLTFromDocId() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// MLT from doc1 content field (Java Programming), server fetches text and should return similar Java documents
		Search search = new Search(MLT_TEST);
		search.addQuery(MoreLikeThisQuery.forDocuments(List.of("content"), "1").setMinDocFreq(1).setMinTermFreq(1));
		search.setAmount(12);
		SearchResult searchResult = zuliaWorkPool.search(search);

		Assertions.assertTrue(searchResult.getTotalHits() > 0, "MLT from doc ID should return results");

		// Should return other Java documents with doc1 itself is excluded by default
		Set<String> resultIds = Set.copyOf(searchResult.getUniqueIds());
		Assertions.assertFalse(resultIds.contains("1"), "Source doc should be excluded by default, got: " + resultIds);
		Assertions.assertTrue(resultCategories(searchResult).contains("java"), "MLT from Java doc should find other Java docs, got: " + resultIds);
	}

	@Test
	@Order(5)
	public void lexicalMLTMultipleDocIds() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// MLT from doc1 (Java) + doc5 (Python) (combines terms from both)
		Search search = new Search(MLT_TEST);
		search.addQuery(MoreLikeThisQuery.forDocuments(List.of("content"), "1", "5").setMinDocFreq(1).setMinTermFreq(1));
		search.setAmount(12);
		SearchResult searchResult = zuliaWorkPool.search(search);

		Assertions.assertTrue(searchResult.getTotalHits() > 0, "MLT from multiple doc IDs should return results");

		// Should find both Java and Python docs since we're combining terms
		Set<String> categories = resultCategories(searchResult);
		Assertions.assertTrue(categories.containsAll(Set.of("java", "python")),
				"MLT from Java+Python docs should find related docs in both categories, got: " + categories);
	}

	@Test
	@Order(6)
	public void vectorOnlyMLTFromDocIds() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// Pure vector MLT: find docs with similar embeddings to doc1 (Java, vector [0.9, 0.1, 0, 0])
		Search search = new Search(MLT_TEST);
		search.addQuery(new MoreLikeThisQuery().addDocumentId("1").setVectorField("embedding").setVectorTopN(5));
		search.setAmount(12);
		SearchResult searchResult = zuliaWorkPool.search(search);

		Assertions.assertTrue(searchResult.getTotalHits() > 0, "Vector-only MLT should return results");

		// Doc1 itself is excluded by default.  The closest remaining vectors are all Java documents (doc12 [0.88,0.12,0,0], doc2 [0.85,0.15,0,0])
		Assertions.assertFalse(searchResult.getUniqueIds().contains("1"), "Source doc should be excluded by default");
		Assertions.assertEquals(Set.of("java"), resultCategories(searchResult, 1),
				"Top vector MLT result should be a Java doc, got: " + searchResult.getUniqueIds().getFirst());
	}

	@Test
	@Order(7)
	public void vectorOnlyMLTFromMultipleDocIds() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// Average vectors from doc1 [0.9,0.1,0,0] and doc3 [0.8,0.1,0.1,0]
		// Centroid ≈ [0.85, 0.1, 0.05, 0] normalized to unit
		Search search = new Search(MLT_TEST);
		search.addQuery(new MoreLikeThisQuery().addDocumentId("1").addDocumentId("3").setVectorField("embedding").setVectorTopN(5));
		search.setAmount(12);
		SearchResult searchResult = zuliaWorkPool.search(search);

		Assertions.assertTrue(searchResult.getTotalHits() > 0, "Vector MLT from multiple docs should return results");

		// All top results should be Java-related (high first component)
		Assertions.assertEquals(Set.of("java"), resultCategories(searchResult, 3),
				"Top vector results should be Java docs, got: " + searchResult.getUniqueIds());
	}

	@Test
	@Order(8)
	public void hybridMLTFromDocId() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// Hybrid: lexical + vector from doc1 (Java Programming)
		Search search = new Search(MLT_TEST);
		search.addQuery(
				new MoreLikeThisQuery("content").addDocumentId("1").setVectorField("embedding").setVectorTopN(5).setMinDocFreq(1).setMinTermFreq(1));
		search.setAmount(12);
		SearchResult searchResult = zuliaWorkPool.search(search);

		Assertions.assertTrue(searchResult.getTotalHits() > 0, "Hybrid MLT should return results");

		// Docs matching both text AND vector should rank highest (other Java documents)
		Assertions.assertEquals(Set.of("java"), resultCategories(searchResult, 4),
				"Top hybrid results should be Java docs, got: " + searchResult.getUniqueIds());
	}

	@Test
	@Order(9)
	public void userSuppliedTextAndVector() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// User supplies both text and vector (no fetch needed)
		Search search = new Search(MLT_TEST);
		search.addQuery(new MoreLikeThisQuery("content").addLikeText("java programming object oriented design patterns")
				.addLikeVector(new float[] { 0.9f, 0.1f, 0.0f, 0.0f }).setVectorField("embedding").setVectorTopN(5).setMinDocFreq(1).setMinTermFreq(1));
		search.setAmount(12);
		SearchResult searchResult = zuliaWorkPool.search(search);

		Assertions.assertTrue(searchResult.getTotalHits() > 0, "User-supplied text+vector MLT should return results");

		// Should find Java documents
		Assertions.assertEquals(Set.of("java"), resultCategories(searchResult, 4),
				"Top results from Java text+vector should be Java docs, got: " + searchResult.getUniqueIds());
	}

	@Test
	@Order(10)
	public void userSuppliedVectorOnly() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// Pure vector from user-supplied vector (no fetch)
		Search search = new Search(MLT_TEST);
		search.addQuery(new MoreLikeThisQuery().addLikeVector(new float[] { 0.0f, 0.2f, 0.8f, 0.0f }).setVectorField("embedding").setVectorTopN(5));
		search.setAmount(12);
		SearchResult searchResult = zuliaWorkPool.search(search);

		Assertions.assertTrue(searchResult.getTotalHits() > 0, "User-supplied vector MLT should return results");

		// Vector [0, 0.2, 0.8, 0] is closest to ML docs (doc8 [0,0.2,0.8,0], doc9 [0,0.1,0.9,0])
		Assertions.assertEquals(Set.of("ml"), resultCategories(searchResult, 1),
				"Top result from ML-like vector should be an ML doc, got: " + searchResult.getUniqueIds().getFirst());
	}

	@Test
	@Order(11)
	public void mixedInputs() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// Mix: doc ID + user text + user vector
		Search search = new Search(MLT_TEST);
		search.addQuery(new MoreLikeThisQuery("content").addDocumentId("1") // fetches Java text + vector
				.addLikeText("machine learning artificial intelligence neural networks") // adds ML terms
				.addLikeVector(new float[] { 0.0f, 0.0f, 1.0f, 0.0f }) // adds ML vector
				.setVectorField("embedding").setVectorTopN(10).setMinDocFreq(1).setMinTermFreq(1));
		search.setAmount(12);
		SearchResult searchResult = zuliaWorkPool.search(search);

		Assertions.assertTrue(searchResult.getTotalHits() > 0, "Mixed input MLT should return results");

		// Should find both Java documents (from doc ID text) and ML docs (from user text + vector). Source doc 1 is excluded
		Set<String> resultIds = Set.copyOf(searchResult.getUniqueIds());
		Assertions.assertFalse(resultIds.contains("1"), "Source doc should be excluded by default, got: " + resultIds);
		Set<String> categories = resultCategories(searchResult);
		Assertions.assertTrue(categories.contains("java"), "Mixed MLT should find Java docs from the source doc text, got: " + categories);
		Assertions.assertTrue(categories.contains("ml"), "Mixed MLT should find ML docs from the user text and vector, got: " + categories);
	}

	@Test
	@Order(12)
	public void mltWithPostFilter() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// MLT + post-filter: find Java-like docs but only in "java" category
		Search search = new Search(MLT_TEST);
		search.addQuery(MoreLikeThisQuery.forText(List.of("content"), "programming language dynamic typing scripting").setMinDocFreq(1).setMinTermFreq(1));
		search.addQuery(new FilterQuery("category:java"));
		search.setAmount(12);
		SearchResult searchResult = zuliaWorkPool.search(search);

		// All results should be Java category
		for (var result : searchResult.getCompleteResults()) {
			Document doc = result.getDocument();
			Assertions.assertEquals("java", doc.getString("category"), "All filtered results should be java category");
		}
	}

	@Test
	@Order(13)
	public void mltMultipleFields() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// MLT across multiple text fields (title + content)
		Search search = new Search(MLT_TEST);
		search.addQuery(
				MoreLikeThisQuery.forText(List.of("title", "content"), "java programming design").setMinDocFreq(1).setMinTermFreq(1).setMaxQueryTerms(30));
		search.setAmount(12);
		SearchResult searchResult = zuliaWorkPool.search(search);

		Assertions.assertTrue(searchResult.getTotalHits() > 0, "Multi-field MLT should return results");
	}

	@Test
	@Order(14)
	public void mltParameterTuning() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// Test with high maxQueryTerms — should produce more diverse results
		Search search = new Search(MLT_TEST);
		search.addQuery(MoreLikeThisQuery.forText(List.of("content"), "java programming object oriented development design patterns language").setMinDocFreq(1)
				.setMinTermFreq(1).setMaxQueryTerms(50));
		search.setAmount(12);
		SearchResult highTermResult = zuliaWorkPool.search(search);

		// Test with very low maxQueryTerms — should produce fewer, more focused results
		search = new Search(MLT_TEST);
		search.addQuery(MoreLikeThisQuery.forText(List.of("content"), "java programming object oriented development design patterns language").setMinDocFreq(1)
				.setMinTermFreq(1).setMaxQueryTerms(2));
		search.setAmount(12);
		SearchResult lowTermResult = zuliaWorkPool.search(search);

		Assertions.assertTrue(highTermResult.getTotalHits() > 0, "High maxQueryTerms MLT should return results");
		Assertions.assertTrue(lowTermResult.getTotalHits() > 0, "Low maxQueryTerms MLT should return results");
		// More query terms generally means more results (or at least equal)
		Assertions.assertTrue(highTermResult.getTotalHits() >= lowTermResult.getTotalHits(),
				"More query terms should find at least as many docs: high=" + highTermResult.getTotalHits() + " low=" + lowTermResult.getTotalHits());
	}

	@Test
	@Order(15)
	public void mltMinWordLen() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// With high minWordLen, short words like "java" (4 chars) should be excluded
		Search search = new Search(MLT_TEST);
		search.addQuery(MoreLikeThisQuery.forText(List.of("content"), "java programming object oriented design patterns").setMinDocFreq(1).setMinTermFreq(1)
				.setMinWordLen(8) // only words 8+ chars
		);
		search.setAmount(12);
		SearchResult searchResult = zuliaWorkPool.search(search);

		// Should still work but with fewer matching terms (only "programming", "oriented", "patterns")
		// May return fewer results since key terms are excluded
		Assertions.assertNotNull(searchResult, "MLT with minWordLen should not throw");
	}

	@Test
	@Order(16)
	public void mltEmptyResultHandling() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// MLT with text that has no overlap with indexed content
		Search search = new Search(MLT_TEST);
		search.addQuery(MoreLikeThisQuery.forText(List.of("content"), "xylophone zephyr quixotic").setMinDocFreq(1).setMinTermFreq(1));
		search.setAmount(12);
		SearchResult searchResult = zuliaWorkPool.search(search);

		// Should return 0 results gracefully (no errors)
		Assertions.assertEquals(0, searchResult.getTotalHits(), "MLT with no term overlap should return 0 results");
	}

	@Test
	@Order(17)
	public void mltForDocumentsConvenienceFactory() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// Test the convenience factory methods
		Search search = new Search(MLT_TEST);
		search.addQuery(MoreLikeThisQuery.forDocuments(List.of("content"), "1", "2").setMinDocFreq(1).setMinTermFreq(1));
		search.setAmount(12);
		SearchResult searchResult = zuliaWorkPool.search(search);

		Assertions.assertTrue(searchResult.getTotalHits() > 0, "forDocuments factory should return results");

		// Both source docs are excluded; the combined Java terms should rank the remaining Java docs on top
		Set<String> resultIds = Set.copyOf(searchResult.getUniqueIds());
		Assertions.assertFalse(resultIds.contains("1") || resultIds.contains("2"), "Source docs should be excluded by default, got: " + resultIds);
		Assertions.assertEquals(Set.of("java"), resultCategories(searchResult, 2),
				"Top results from two Java source docs should be Java docs, got: " + searchResult.getUniqueIds());
	}

	@Test
	@Order(18)
	public void mltScoresDescending() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// Verify results are returned in descending score order
		Search search = new Search(MLT_TEST);
		search.addQuery(MoreLikeThisQuery.forText(List.of("content"), "java programming object oriented design patterns development").setMinDocFreq(1)
				.setMinTermFreq(1));
		search.setAmount(12);
		SearchResult searchResult = zuliaWorkPool.search(search);

		double previousScore = Double.MAX_VALUE;
		for (var result : searchResult.getCompleteResults()) {
			Assertions.assertTrue(result.getScore() <= previousScore,
					"Results should be in descending score order: " + result.getScore() + " > " + previousScore);
			previousScore = result.getScore();
		}
	}

	@Test
	@Order(19)
	public void mltNoDuplicates() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// Verify no duplicate IDs in results
		Search search = new Search(MLT_TEST);
		search.addQuery(
				new MoreLikeThisQuery("content").addDocumentId("1").setVectorField("embedding").setVectorTopN(10).setMinDocFreq(1).setMinTermFreq(1));
		search.setAmount(12);
		SearchResult searchResult = zuliaWorkPool.search(search);

		Set<String> uniqueIds = Set.copyOf(searchResult.getUniqueIds());
		Assertions.assertEquals(searchResult.getUniqueIds().size(), uniqueIds.size(), "Results should have no duplicate IDs");
	}

	@Test
	@Order(20)
	public void multiIndexLexicalMLTFromDocId() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// Source doc lives in MLT_TEST and querying both indexes so should pull text from doc1 and find Java-similar docs in both
		Search search = new Search(MLT_TEST, MLT_TEST_SECONDARY).setRealtime(true);
		search.addQuery(MoreLikeThisQuery.forDocuments(List.of("content"), "1").setMinDocFreq(1).setMinTermFreq(1));
		search.setAmount(20);
		SearchResult searchResult = zuliaWorkPool.search(search);

		Assertions.assertTrue(searchResult.getTotalHits() > 0, "Multi-index MLT from doc ID should return results");

		Set<String> indexes = resultIndexes(searchResult);
		Assertions.assertTrue(indexes.containsAll(Set.of(MLT_TEST, MLT_TEST_SECONDARY)),
				"Multi-index MLT should return docs from both indexes, got: " + indexes);
		Assertions.assertTrue(resultCategories(searchResult).contains("java"),
				"Multi-index MLT should return Java-related docs, got: " + searchResult.getUniqueIds());
	}

	@Test
	@Order(21)
	public void multiIndexMissingDocIdFails() {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		Search search = new Search(MLT_TEST, MLT_TEST_SECONDARY);
		search.addQuery(MoreLikeThisQuery.forDocuments(List.of("content"), "doesNotExist").setMinDocFreq(1).setMinTermFreq(1));
		search.setAmount(5);

		Exception ex = Assertions.assertThrows(Exception.class, () -> zuliaWorkPool.search(search));
		Assertions.assertTrue(ex.getMessage() != null && ex.getMessage().contains("doesNotExist"),
				"Error should name the missing docId, got: " + ex.getMessage());
	}

	@Test
	@Order(22)
	public void hybridMLTWithWeights() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// Source doc 6 (Python Data Science) has opposing affinities: its vector is closest to doc 5 (python),
		// but its text shares the rarest terms (machine, learning, data, analysis) with the ML docs

		// With the vector clause dominant, the nearest vector wins.
		// Disable the maxDocFreqPct guard: the discriminating terms (machine, learning, data) appear in >25% of this tiny index.
		Search search = new Search(MLT_TEST);
		search.addQuery(new MoreLikeThisQuery("content").addDocumentId("6").setVectorField("embedding").setVectorTopN(5).setMinDocFreq(1).setMinTermFreq(1)
				.setTextWeight(0.01f).setVectorWeight(10.0f).setMaxDocFreqPct(100));
		search.setAmount(5);
		SearchResult vectorHeavy = zuliaWorkPool.search(search);
		Assertions.assertTrue(vectorHeavy.getTotalHits() > 0, "Vector-weighted hybrid MLT should return results");
		Assertions.assertEquals(Set.of("python"), resultCategories(vectorHeavy, 1),
				"Vector-heavy weights should rank the nearest vector (doc 5, python) first, got: " + vectorHeavy.getUniqueIds().getFirst());

		// Same query with text dominant, the lexically closest ML docs win
		search = new Search(MLT_TEST);
		search.addQuery(new MoreLikeThisQuery("content").addDocumentId("6").setVectorField("embedding").setVectorTopN(5).setMinDocFreq(1).setMinTermFreq(1)
				.setTextWeight(10.0f).setVectorWeight(0.01f).setMaxDocFreqPct(100));
		search.setAmount(5);
		SearchResult textHeavy = zuliaWorkPool.search(search);
		Assertions.assertTrue(textHeavy.getTotalHits() > 0, "Text-weighted hybrid MLT should return results");
		Assertions.assertEquals(Set.of("ml"), resultCategories(textHeavy, 1),
				"Text-heavy weights should rank the lexically closest ML doc first, got: " + textHeavy.getUniqueIds().getFirst());
	}

	@Test
	@Order(23)
	public void mltUnknownTextFieldFails() {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		Search search = new Search(MLT_TEST);
		search.addQuery(MoreLikeThisQuery.forText(List.of("nonExistentField"), "java programming"));
		search.setAmount(5);

		Exception ex = Assertions.assertThrows(Exception.class, () -> zuliaWorkPool.search(search));
		Assertions.assertTrue(ex.getMessage() != null && ex.getMessage().contains("nonExistentField"),
				"Error should name the missing field, got: " + ex.getMessage());
	}

	@Test
	@Order(24)
	public void mltVectorFieldOnNonVectorFieldFails() {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// "content" is a STRING field, not a vector field
		Search search = new Search(MLT_TEST);
		search.addQuery(new MoreLikeThisQuery().addLikeVector(new float[] { 0.1f, 0.2f, 0.3f, 0.4f }).setVectorField("content").setVectorTopN(5));
		search.setAmount(5);

		Exception ex = Assertions.assertThrows(Exception.class, () -> zuliaWorkPool.search(search));
		Assertions.assertTrue(ex.getMessage() != null && ex.getMessage().contains("content"),
				"Error should name the bad vector field, got: " + ex.getMessage());
	}

	@Test
	@Order(25)
	public void mltSourceDocLimitEnforced() {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		MoreLikeThisQuery mlt = new MoreLikeThisQuery("content");
		for (int i = 0; i < 101; i++) {
			mlt.addDocumentId(String.valueOf(i));
		}
		Search search = new Search(MLT_TEST);
		search.addQuery(mlt);
		search.setAmount(5);

		Exception ex = Assertions.assertThrows(Exception.class, () -> zuliaWorkPool.search(search));
		Assertions.assertTrue(ex.getMessage() != null && ex.getMessage().contains("source document limit"),
				"Error should mention the source document limit, got: " + ex.getMessage());
	}

	@Test
	@Order(26)
	public void wildcardIndexMLTFromDocId() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// Wildcard index pattern matching both indexes with source doc living in MLT_TEST
		Search search = new Search(MLT_TEST + "*").setRealtime(true);
		search.addQuery(MoreLikeThisQuery.forDocuments(List.of("content"), "1").setMinDocFreq(1).setMinTermFreq(1));
		search.setAmount(20);
		SearchResult searchResult = zuliaWorkPool.search(search);

		Assertions.assertTrue(searchResult.getTotalHits() > 0, "Wildcard index MLT should return results");

		Set<String> indexes = resultIndexes(searchResult);
		Assertions.assertTrue(indexes.containsAll(Set.of(MLT_TEST, MLT_TEST_SECONDARY)),
				"Wildcard index MLT should return docs from both indexes, got: " + indexes);
		Assertions.assertTrue(resultCategories(searchResult).contains("java"),
				"Wildcard index MLT should return Java-related docs, got: " + searchResult.getUniqueIds());
	}

	@Test
	@Order(27)
	public void mltLikeTextWithoutTextFieldsFails() {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// likeText can never be used without text fields to analyze
		Search search = new Search(MLT_TEST);
		search.addQuery(new MoreLikeThisQuery().addLikeText("java programming").setVectorField("embedding")
				.addLikeVector(new float[] { 0.9f, 0.1f, 0.0f, 0.0f }));
		search.setAmount(5);

		Exception ex = Assertions.assertThrows(Exception.class, () -> zuliaWorkPool.search(search));
		Assertions.assertTrue(ex.getMessage() != null && ex.getMessage().contains("no text fields"),
				"Error should say text fields are missing, got: " + ex.getMessage());
	}

	@Test
	@Order(28)
	public void mltLikeVectorWithoutVectorFieldFails() {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// likeVector can never be used without a vector field
		Search search = new Search(MLT_TEST);
		search.addQuery(new MoreLikeThisQuery("content").addLikeText("java programming").addLikeVector(new float[] { 0.9f, 0.1f, 0.0f, 0.0f })
				.setMinDocFreq(1).setMinTermFreq(1));
		search.setAmount(5);

		Exception ex = Assertions.assertThrows(Exception.class, () -> zuliaWorkPool.search(search));
		Assertions.assertTrue(ex.getMessage() != null && ex.getMessage().contains("no vector field"),
				"Error should say the vector field is missing, got: " + ex.getMessage());
	}

	@Test
	@Order(29)
	public void mltIncludeSourceDocs() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// With includeSourceDocs, doc1 should come back and rank first on its own vector
		Search search = new Search(MLT_TEST);
		search.addQuery(new MoreLikeThisQuery().addDocumentId("1").setVectorField("embedding").setVectorTopN(5).setIncludeSourceDocs(true));
		search.setAmount(12);
		SearchResult searchResult = zuliaWorkPool.search(search);

		Assertions.assertTrue(searchResult.getTotalHits() > 0, "Vector MLT including source docs should return results");
		Assertions.assertEquals("1", searchResult.getUniqueIds().getFirst(),
				"Source doc should rank first on its own vector when included");
	}

	@Test
	@Order(30)
	public void mltMinShouldMatch() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// Without mm, any single shared term matches (e.g. "programming" matches Python/JS docs).
		// Disable the maxDocFreqPct guard so all six terms survive on this tiny index; mm=4 needs them present.
		Search search = new Search(MLT_TEST);
		search.addQuery(MoreLikeThisQuery.forText(List.of("content"), "java programming object oriented design patterns").setMinDocFreq(1).setMinTermFreq(1)
				.setMaxDocFreqPct(100));
		search.setAmount(12);
		SearchResult noMmResult = zuliaWorkPool.search(search);

		// With mm 4, a doc must match at least 4 of the selected terms so only the Java documents qualify
		search = new Search(MLT_TEST);
		search.addQuery(MoreLikeThisQuery.forText(List.of("content"), "java programming object oriented design patterns").setMinDocFreq(1).setMinTermFreq(1)
				.setMinShouldMatch(4).setMaxDocFreqPct(100));
		search.setAmount(12);
		SearchResult mmResult = zuliaWorkPool.search(search);

		Assertions.assertTrue(mmResult.getTotalHits() > 0, "MLT with minShouldMatch should return results");
		Assertions.assertTrue(mmResult.getTotalHits() < noMmResult.getTotalHits(),
				"minShouldMatch should reduce matches: mm=" + mmResult.getTotalHits() + " noMm=" + noMmResult.getTotalHits());

		for (var result : mmResult.getCompleteResults()) {
			Assertions.assertEquals("java", result.getDocument().getString("category"),
					"Docs matching 4+ terms should all be Java docs, got: " + result.getUniqueId());
		}
	}

	@Test
	@Order(31)
	public void multiShardMLT() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// 3 shards across 3 nodes that exercises per-shard lazy rewrite and cross-shard score merging
		String multiShardIndex = "mltTestMultiShard";
		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("content");
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("title").indexAs(DefaultAnalyzers.STANDARD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("content").indexAs(DefaultAnalyzers.STANDARD));
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("category").indexAs(DefaultAnalyzers.LC_KEYWORD).facet().sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createUnitVector("embedding").index());
		indexConfig.setIndexName(multiShardIndex);
		indexConfig.setNumberOfShards(3);
		indexConfig.setShardCommitInterval(20);
		zuliaWorkPool.createIndex(indexConfig);

		indexRecord(multiShardIndex, "1", "Java Programming", "java programming language object oriented design patterns inheritance polymorphism", "java",
				new float[] { 0.9f, 0.1f, 0.0f, 0.0f });
		indexRecord(multiShardIndex, "2", "Java Development",
				"java development software engineering object oriented design best practices enterprise application development programming", "java",
				new float[] { 0.85f, 0.15f, 0.0f, 0.0f });
		indexRecord(multiShardIndex, "5", "Python Scripting",
				"python scripting language dynamic typing interpreted programming easy to learn beginner friendly", "python",
				new float[] { 0.1f, 0.9f, 0.0f, 0.0f });
		indexRecord(multiShardIndex, "8", "Machine Learning Fundamentals",
				"machine learning artificial intelligence neural networks deep learning training data models prediction classification", "ml",
				new float[] { 0.0f, 0.2f, 0.8f, 0.0f });
		indexRecord(multiShardIndex, "12", "Advanced Java",
				"java programming advanced topics concurrency multithreading object oriented design patterns factory singleton observer", "java",
				new float[] { 0.88f, 0.12f, 0.0f, 0.0f });

		// Lexical MLT from doc 1 across shards
		Search search = new Search(multiShardIndex).setRealtime(true);
		search.addQuery(MoreLikeThisQuery.forDocuments(List.of("content"), "1").setMinDocFreq(1).setMinTermFreq(1));
		search.setAmount(10);
		SearchResult lexicalResult = zuliaWorkPool.search(search);

		Assertions.assertTrue(lexicalResult.getTotalHits() > 0, "Multi-shard lexical MLT should return results");
		Set<String> lexicalIds = Set.copyOf(lexicalResult.getUniqueIds());
		Assertions.assertFalse(lexicalIds.contains("1"), "Source doc should be excluded, got: " + lexicalIds);
		Assertions.assertTrue(resultCategories(lexicalResult).contains("java"),
				"Multi-shard MLT from Java doc should find other Java docs, got: " + lexicalIds);

		// Hybrid MLT from doc 1 across shards
		search = new Search(multiShardIndex).setRealtime(true);
		search.addQuery(new MoreLikeThisQuery("content").addDocumentId("1").setVectorField("embedding").setVectorTopN(5).setMinDocFreq(1).setMinTermFreq(1));
		search.setAmount(10);
		SearchResult hybridResult = zuliaWorkPool.search(search);

		Assertions.assertTrue(hybridResult.getTotalHits() > 0, "Multi-shard hybrid MLT should return results");
		Assertions.assertEquals(Set.of("java"), resultCategories(hybridResult, 1),
				"Top multi-shard hybrid result should be a Java doc, got: " + hybridResult.getUniqueIds().getFirst());
	}

	@Test
	@Order(32)
	public void mltNoSourcesFails() {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// Text fields alone are not enough: a source (document ids, like text, or like vectors) is required
		Search search = new Search(MLT_TEST);
		search.addQuery(new MoreLikeThisQuery("content"));
		search.setAmount(5);

		Exception ex = Assertions.assertThrows(Exception.class, () -> zuliaWorkPool.search(search));
		Assertions.assertTrue(ex.getMessage() != null && ex.getMessage().contains("at least one source"),
				"Error should say a source is required, got: " + ex.getMessage());
	}

	@Test
	@Order(33)
	public void mltSourceDocsWithNoContentFails() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// Source doc with neither text in the analyzed field nor a vector resolves to no content
		indexRecord("13", "Empty Doc", null, "misc", null);

		Search search = new Search(MLT_TEST).setRealtime(true);
		search.addQuery(new MoreLikeThisQuery("content").setVectorField("embedding").addDocumentId("13"));
		search.setAmount(5);

		Exception ex = Assertions.assertThrows(Exception.class, () -> zuliaWorkPool.search(search));
		Assertions.assertTrue(ex.getMessage() != null && ex.getMessage().contains("contained no text in fields [content] and no vectors in field <embedding>"),
				"Error should say the source docs yielded no content, got: " + ex.getMessage());
	}

	@Test
	@Order(34)
	public void sourceIndexFetchesFromDifferentIndex() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// doc "100" lives only in the secondary index; search the primary index for docs similar to it by pointing sourceIndex at the secondary.
		// Disable the maxDocFreqPct guard: on this tiny index the shared Java terms exceed 25% df, especially after later tests add more Java docs.
		Search search = new Search(MLT_TEST).setRealtime(true);
		search.addQuery(new MoreLikeThisQuery("content").addDocumentId("100").addSourceIndex(MLT_TEST_SECONDARY).setMinDocFreq(1).setMinTermFreq(1)
				.setMaxDocFreqPct(100));
		search.setAmount(10);
		SearchResult searchResult = zuliaWorkPool.search(search);

		Assertions.assertTrue(searchResult.getTotalHits() > 0, "MLT with sourceIndex should fetch the source doc from the other index and return results");
		// the search runs only on the queried index even though the source doc came from a different one
		Assertions.assertEquals(Set.of(MLT_TEST), resultIndexes(searchResult), "Results should come only from the queried index, got: " + resultIndexes(searchResult));
		// doc 100 is Kotlin/Java, so Java docs in the primary index should match
		Assertions.assertTrue(resultCategories(searchResult).contains("java"),
				"Docs similar to the Java/Kotlin source should include java, got: " + searchResult.getUniqueIds());
	}

	@Test
	@Order(35)
	public void sourceIndexIgnoresQueriedIndexesForFetch() {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// doc "1" exists in the queried index but not the source index; with sourceIndex set the queried index is not used for fetching, so it must fail
		Search search = new Search(MLT_TEST).setRealtime(true);
		search.addQuery(new MoreLikeThisQuery("content").addDocumentId("1").addSourceIndex(MLT_TEST_SECONDARY).setMinDocFreq(1).setMinTermFreq(1));
		search.setAmount(5);

		Exception ex = Assertions.assertThrows(Exception.class, () -> zuliaWorkPool.search(search));
		Assertions.assertTrue(ex.getMessage() != null && ex.getMessage().contains("source indexes"),
				"Error should reference the source indexes, got: " + ex.getMessage());
	}

	@Test
	@Order(36)
	public void sourceIndexPriorityOrder() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// same id in two indexes with different content; the first source index in the list wins
		indexRecord(MLT_TEST, "shared1", "Shared Java", "java programming object oriented inheritance polymorphism design patterns", "java", null);
		indexRecord(MLT_TEST_SECONDARY, "shared1", "Shared Python", "python scripting dynamic typing interpreted pandas numpy data science", "python", null);

		// secondary first -> python source doc -> python-similar docs win
		Search pythonFirst = new Search(MLT_TEST).setRealtime(true);
		pythonFirst.addQuery(new MoreLikeThisQuery("content").addDocumentId("shared1").addSourceIndex(MLT_TEST_SECONDARY).addSourceIndex(MLT_TEST)
				.setMinDocFreq(1).setMinTermFreq(1));
		pythonFirst.setAmount(5);
		SearchResult pythonResult = zuliaWorkPool.search(pythonFirst);
		Assertions.assertTrue(resultCategories(pythonResult, 1).contains("python"),
				"Secondary-first source order should use the python source doc, got: " + pythonResult.getUniqueIds());

		// primary first -> java source doc -> java-similar docs win
		Search javaFirst = new Search(MLT_TEST).setRealtime(true);
		javaFirst.addQuery(new MoreLikeThisQuery("content").addDocumentId("shared1").addSourceIndex(MLT_TEST).addSourceIndex(MLT_TEST_SECONDARY)
				.setMinDocFreq(1).setMinTermFreq(1));
		javaFirst.setAmount(5);
		SearchResult javaResult = zuliaWorkPool.search(javaFirst);
		Assertions.assertTrue(resultCategories(javaResult, 1).contains("java"),
				"Primary-first source order should use the java source doc, got: " + javaResult.getUniqueIds());
	}

	@Test
	@Order(37)
	public void sourceIndexDisjointDoesNotExcludeQueriedDoc() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// same id in the queried index and the (disjoint) source index, both Java content
		indexRecord(MLT_TEST, "dup1", "Dup Java Primary", "java programming object oriented inheritance polymorphism design patterns concurrency", "java", null);
		indexRecord(MLT_TEST_SECONDARY, "dup1", "Dup Java Secondary", "java programming object oriented inheritance polymorphism design patterns concurrency",
				"java", null);

		// source doc "dup1" is fetched only from the secondary index, which is not queried; with includeSourceDocs=false (default)
		// the primary index's unrelated "dup1" must NOT be excluded, since it is a different document that only shares the id
		Search search = new Search(MLT_TEST).setRealtime(true);
		search.addQuery(new MoreLikeThisQuery("content").addDocumentId("dup1").addSourceIndex(MLT_TEST_SECONDARY).setMinDocFreq(1).setMinTermFreq(1));
		search.setAmount(20);
		SearchResult searchResult = zuliaWorkPool.search(search);

		Assertions.assertTrue(searchResult.getUniqueIds().contains("dup1"),
				"A same-id doc in the queried index must not be excluded when the source doc came from a disjoint source index, got: "
						+ searchResult.getUniqueIds());
	}

	@Test
	@Order(38)
	public void mltMaxDocFreqPct() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// Dedicated single-shard index with deterministic document frequencies: "report" is boilerplate in all 8 docs
		// (100% df) while each topic term appears in at most 3 docs.
		String pctIndex = "mltMaxDocFreqPct";
		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("content");
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("content").indexAs(DefaultAnalyzers.STANDARD));
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("category").indexAs(DefaultAnalyzers.LC_KEYWORD).facet().sort());
		indexConfig.setIndexName(pctIndex);
		indexConfig.setNumberOfShards(1);
		indexConfig.setShardCommitInterval(20);
		zuliaWorkPool.createIndex(indexConfig);

		indexRecord(pctIndex, "a1", "A1", "report alpha apple apricot avocado", "alpha", null);
		indexRecord(pctIndex, "a2", "A2", "report alpha apple apricot fruit", "alpha", null);
		indexRecord(pctIndex, "a3", "A3", "report alpha apple harvest", "alpha", null);
		indexRecord(pctIndex, "b1", "B1", "report beta banana blueberry blackberry", "beta", null);
		indexRecord(pctIndex, "b2", "B2", "report beta banana blueberry market", "beta", null);
		indexRecord(pctIndex, "b3", "B3", "report beta banana season", "beta", null);
		indexRecord(pctIndex, "c1", "C1", "report gamma grape", "gamma", null);
		indexRecord(pctIndex, "c2", "C2", "report delta date", "delta", null);

		// Guard disabled (100): the boilerplate "report" is kept, so the query reaches docs in every category.
		Search disabled = new Search(pctIndex).setRealtime(true);
		disabled.addQuery(
				MoreLikeThisQuery.forText(List.of("content"), "report alpha apple apricot").setMinDocFreq(1).setMinTermFreq(1).setMaxDocFreqPct(100));
		disabled.setAmount(20);
		SearchResult disabledResult = zuliaWorkPool.search(disabled);

		Assertions.assertEquals(8, disabledResult.getTotalHits(), "With the guard disabled, the boilerplate term should match every doc");
		Assertions.assertTrue(resultCategories(disabledResult).contains("beta"),
				"Boilerplate term should pull in unrelated categories when the guard is disabled, got: " + resultCategories(disabledResult));

		// Guard at 50%: "report" appears in 100% of docs and is dropped, leaving only the alpha-specific terms, so only alpha docs match.
		Search guarded = new Search(pctIndex).setRealtime(true);
		guarded.addQuery(
				MoreLikeThisQuery.forText(List.of("content"), "report alpha apple apricot").setMinDocFreq(1).setMinTermFreq(1).setMaxDocFreqPct(50));
		guarded.setAmount(20);
		SearchResult guardedResult = zuliaWorkPool.search(guarded);

		Assertions.assertTrue(guardedResult.getTotalHits() > 0, "Guarded MLT should still match on the topic-specific terms");
		Assertions.assertTrue(guardedResult.getTotalHits() < disabledResult.getTotalHits(),
				"maxDocFreqPct should drop the boilerplate term and reduce matches: guarded=" + guardedResult.getTotalHits() + " disabled="
						+ disabledResult.getTotalHits());
		Assertions.assertEquals(Set.of("alpha"), resultCategories(guardedResult),
				"With the boilerplate term dropped, only the alpha-specific terms remain, so only alpha docs match, got: " + resultCategories(guardedResult));
	}

	@Test
	@Order(39)
	public void mltMaxDocFreqPctOutOfRangeFails() {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		Search search = new Search(MLT_TEST);
		search.addQuery(MoreLikeThisQuery.forText(List.of("content"), "java programming").setMaxDocFreqPct(150));
		search.setAmount(5);

		Exception ex = Assertions.assertThrows(Exception.class, () -> zuliaWorkPool.search(search));
		Assertions.assertTrue(ex.getMessage() != null && ex.getMessage().contains("maxDocFreqPct"),
				"Error should reference maxDocFreqPct, got: " + ex.getMessage());
	}

	@Test
	@Order(50)
	public void restart() throws Exception {
		nodeExtension.restartNodes();
	}

	@Test
	@Order(51)
	public void confirmAfterRestart() throws Exception {
		// Re-run key tests after restart to verify persistence
		lexicalMLTFromText();
		lexicalMLTFromDocId();
		vectorOnlyMLTFromDocIds();
		hybridMLTFromDocId();
		sourceIndexFetchesFromDifferentIndex();
	}
}
