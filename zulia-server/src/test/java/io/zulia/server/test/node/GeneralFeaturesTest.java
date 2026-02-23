package io.zulia.server.test.node;

import io.zulia.DefaultAnalyzers;
import io.zulia.client.command.BatchDelete;
import io.zulia.client.command.BatchFetch;
import io.zulia.client.command.FetchAllAssociated;
import io.zulia.client.command.DeleteFromIndex;
import io.zulia.client.command.Fetch;
import io.zulia.client.command.Store;
import io.zulia.client.command.builder.BatchFetchGroupBuilder;
import io.zulia.client.command.builder.FilterQuery;
import io.zulia.client.command.builder.Search;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.AssociatedResult;
import io.zulia.client.result.BatchFetchResult;
import io.zulia.client.result.FetchResult;
import io.zulia.client.result.SearchResult;
import io.zulia.doc.AssociatedBuilder;
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

import io.zulia.message.ZuliaQuery.FetchType;

import java.util.List;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class GeneralFeaturesTest {

	@RegisterExtension
	static final NodeExtension nodeExtension = new NodeExtension(3);

	public static final String INDEX_NAME = "generalFeatures";

	private static final int UNIQUE_DOCS = 5;

	@Test
	@Order(1)
	public void createIndex() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("title");
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("title").indexAs(DefaultAnalyzers.STANDARD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("category").indexAs(DefaultAnalyzers.STANDARD));
		indexConfig.addFieldConfig(FieldConfigBuilder.createDouble("rating").index().sort());
		indexConfig.setIndexName(INDEX_NAME);
		indexConfig.setNumberOfShards(1);
		indexConfig.setShardCommitInterval(20);

		zuliaWorkPool.createIndex(indexConfig);
	}

	@Test
	@Order(2)
	public void index() throws Exception {
		indexRecord(1, "Java Programming", "tech", 4.5);
		indexRecord(2, "Python Basics", "tech", 3.8);
		indexRecord(3, "Cooking Guide", "food", 4.2);
		indexRecord(4, "Travel Tips", "travel", 3.5);
		indexRecord(5, "Data Science", "tech", 4.9);

		// Store associated text files for docs 1 and 2
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		Store store1 = new Store("1", INDEX_NAME);
		store1.addAssociatedDocument(AssociatedBuilder.newBuilder().setFilename("notes.txt").setDocument("Java is great"));
		zuliaWorkPool.store(store1);

		Store store2 = new Store("2", INDEX_NAME);
		store2.addAssociatedDocument(AssociatedBuilder.newBuilder().setFilename("notes.txt").setDocument("Python is easy"));
		store2.addAssociatedDocument(AssociatedBuilder.newBuilder().setFilename("summary.txt").setDocument("A beginner guide"));
		zuliaWorkPool.store(store2);
	}

	private void indexRecord(int id, String title, String category, double rating) throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		String uniqueId = String.valueOf(id);

		Document mongoDocument = new Document();
		mongoDocument.put("id", uniqueId);
		mongoDocument.put("title", title);
		mongoDocument.put("category", category);
		mongoDocument.put("rating", rating);

		Store s = new Store(uniqueId, INDEX_NAME);
		s.setResultDocument(ResultDocBuilder.newBuilder().setDocument(mongoDocument));
		zuliaWorkPool.store(s);
	}

	@Test
	@Order(3)
	public void documentFieldFilteringTest() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// Whitelist: fetch only specific fields
		Fetch fetch = new Fetch("1", INDEX_NAME);
		fetch.addDocumentField("title");
		fetch.setRealtime(true);
		FetchResult fetchResult = zuliaWorkPool.fetch(fetch);
		Document doc = fetchResult.getDocument();
		Assertions.assertNotNull(doc.getString("title"));
		Assertions.assertNull(doc.getString("category"));
		Assertions.assertNull(doc.get("rating"));
		Assertions.assertNull(doc.getString("id"));

		// Mask: exclude specific fields
		Fetch fetchMasked = new Fetch("1", INDEX_NAME);
		fetchMasked.addDocumentMaskedField("rating");
		fetchMasked.addDocumentMaskedField("category");
		fetchMasked.setRealtime(true);
		FetchResult maskedResult = zuliaWorkPool.fetch(fetchMasked);
		Document maskedDoc = maskedResult.getDocument();
		Assertions.assertNotNull(maskedDoc.getString("title"));
		Assertions.assertNotNull(maskedDoc.getString("id"));
		Assertions.assertNull(maskedDoc.get("rating"));
		Assertions.assertNull(maskedDoc.getString("category"));

		// Search with document fields (whitelist)
		Search search = new Search(INDEX_NAME).setAmount(10).setRealtime(true);
		search.addDocumentFields("title", "rating");
		SearchResult searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(UNIQUE_DOCS, searchResult.getTotalHits());
		for (Document searchDoc : searchResult.getDocuments()) {
			Assertions.assertNotNull(searchDoc.getString("title"));
			Assertions.assertNotNull(searchDoc.get("rating"));
			Assertions.assertNull(searchDoc.getString("category"));
			Assertions.assertNull(searchDoc.getString("id"));
		}

		// Search with masked fields (blacklist)
		Search searchMasked = new Search(INDEX_NAME).setAmount(10);
		searchMasked.addDocumentMaskedField("rating");
		SearchResult maskedSearchResult = zuliaWorkPool.search(searchMasked);
		Assertions.assertEquals(UNIQUE_DOCS, maskedSearchResult.getTotalHits());
		for (Document searchDoc : maskedSearchResult.getDocuments()) {
			Assertions.assertNotNull(searchDoc.getString("title"));
			Assertions.assertNotNull(searchDoc.getString("id"));
			Assertions.assertNull(searchDoc.get("rating"));
		}
	}

	@Test
	@Order(4)
	public void batchFetchTest() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// Batch fetch by unique IDs using custom Fetch objects with realtime
		BatchFetch batchFetch = new BatchFetch();
		for (String id : List.of("1", "3", "5")) {
			Fetch f = new Fetch(id, INDEX_NAME);
			f.setRealtime(true);
			batchFetch.addFetches(List.of(f));
		}
		BatchFetchResult batchResult = zuliaWorkPool.batchFetch(batchFetch);
		List<FetchResult> results = batchResult.getFetchResults();
		Assertions.assertEquals(3, results.size());
		Assertions.assertTrue(results.stream().allMatch(FetchResult::hasResultDocument));

		// Batch fetch from search results
		Search search = new Search(INDEX_NAME).setAmount(10).setRealtime(true);
		search.addQuery(new FilterQuery("category:tech"));
		SearchResult searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(3, searchResult.getTotalHits());

		BatchFetch batchFromSearch = new BatchFetch().addFetchDocumentsFromResults(searchResult);
		BatchFetchResult searchBatchResult = zuliaWorkPool.batchFetch(batchFromSearch);
		List<FetchResult> searchFetchResults = searchBatchResult.getFetchResults();
		Assertions.assertEquals(3, searchFetchResults.size());

		// Convenience method: batch fetch by IDs (works after data is committed)
		BatchFetch batchById = new BatchFetch();
		batchById.addFetchDocumentsFromUniqueIds(List.of("1", "2", "3", "4", "5"), INDEX_NAME);
		BatchFetchResult allResult = zuliaWorkPool.batchFetch(batchById);
		Assertions.assertEquals(UNIQUE_DOCS, allResult.getFetchResults().size());
	}

	@Test
	@Order(5)
	public void batchFetchGroupTest() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// BatchFetchGroupBuilder: fetch all docs with full documents
		BatchFetchGroupBuilder allDocsGroup = new BatchFetchGroupBuilder(INDEX_NAME, List.of("1", "2", "3", "4", "5")).setRealtime(true);
		BatchFetch groupFetch = new BatchFetch().addFetchGroup(allDocsGroup);
		BatchFetchResult groupResult = zuliaWorkPool.batchFetch(groupFetch);
		List<FetchResult> groupResults = groupResult.getFetchResults();
		Assertions.assertEquals(UNIQUE_DOCS, groupResults.size());
		for (FetchResult fr : groupResults) {
			Assertions.assertTrue(fr.hasResultDocument());
			Document doc = fr.getDocument();
			Assertions.assertNotNull(doc.getString("title"));
			Assertions.assertNotNull(doc.getString("category"));
			Assertions.assertNotNull(doc.get("rating"));
		}

		// BatchFetchGroupBuilder with document field filtering (whitelist)
		BatchFetchGroupBuilder fieldsGroup = new BatchFetchGroupBuilder(INDEX_NAME, List.of("1", "2", "3")).addDocumentField("title").setRealtime(true);
		BatchFetch fieldsFetch = new BatchFetch().addFetchGroup(fieldsGroup);
		BatchFetchResult fieldsResult = zuliaWorkPool.batchFetch(fieldsFetch);
		List<FetchResult> fieldsResults = fieldsResult.getFetchResults();
		Assertions.assertEquals(3, fieldsResults.size());
		for (FetchResult fr : fieldsResults) {
			Document doc = fr.getDocument();
			Assertions.assertNotNull(doc.getString("title"));
			Assertions.assertNull(doc.getString("category"), "category should be filtered out");
			Assertions.assertNull(doc.get("rating"), "rating should be filtered out");
		}

		// BatchFetchGroupBuilder with masked fields (blacklist)
		BatchFetchGroupBuilder maskedGroup = new BatchFetchGroupBuilder(INDEX_NAME, List.of("1", "2")).addDocumentMaskedField("rating")
				.addDocumentMaskedField("category").setRealtime(true);
		BatchFetch maskedFetch = new BatchFetch().addFetchGroup(maskedGroup);
		BatchFetchResult maskedResult = zuliaWorkPool.batchFetch(maskedFetch);
		List<FetchResult> maskedResults = maskedResult.getFetchResults();
		Assertions.assertEquals(2, maskedResults.size());
		for (FetchResult fr : maskedResults) {
			Document doc = fr.getDocument();
			Assertions.assertNotNull(doc.getString("title"));
			Assertions.assertNotNull(doc.getString("id"));
			Assertions.assertNull(doc.get("rating"), "rating should be masked");
			Assertions.assertNull(doc.getString("category"), "category should be masked");
		}

		// Mix: BatchFetchGroupBuilder alongside individual Fetch objects
		BatchFetchGroupBuilder mixGroup = new BatchFetchGroupBuilder(INDEX_NAME, List.of("1", "2")).setRealtime(true);
		Fetch individualFetch = new Fetch("5", INDEX_NAME);
		individualFetch.setRealtime(true);
		BatchFetch mixFetch = new BatchFetch().addFetchGroup(mixGroup).addFetches(List.of(individualFetch));
		BatchFetchResult mixResult = zuliaWorkPool.batchFetch(mixFetch);
		Assertions.assertEquals(3, mixResult.getFetchResults().size());

		// Multiple groups with different field profiles in the same batch
		BatchFetchGroupBuilder titleOnlyGroup = new BatchFetchGroupBuilder(INDEX_NAME, List.of("1", "3")).addDocumentField("title").setRealtime(true);
		BatchFetchGroupBuilder fullGroup = new BatchFetchGroupBuilder(INDEX_NAME, List.of("2", "4")).setRealtime(true);
		BatchFetch multiGroupFetch = new BatchFetch().addFetchGroup(titleOnlyGroup).addFetchGroup(fullGroup);
		BatchFetchResult multiGroupResult = zuliaWorkPool.batchFetch(multiGroupFetch);
		List<FetchResult> multiGroupResults = multiGroupResult.getFetchResults();
		Assertions.assertEquals(4, multiGroupResults.size());

		// Fetch with NONE result type returns no documents (would be used for associated documents only)
		BatchFetchGroupBuilder noneGroup = new BatchFetchGroupBuilder(INDEX_NAME, List.of("1", "2")).setResultFetchType(FetchType.NONE).setRealtime(true);
		BatchFetch noneFetch = new BatchFetch().addFetchGroup(noneGroup);
		BatchFetchResult noneResult = zuliaWorkPool.batchFetch(noneFetch);
		List<FetchResult> noneResults = noneResult.getFetchResults();
		Assertions.assertEquals(2, noneResults.size());
		for (FetchResult fr : noneResults) {
			Assertions.assertFalse(fr.hasResultDocument());
		}

		// Verify associated docs were stored correctly using single fetch
		FetchResult verifyResult = zuliaWorkPool.fetch(new FetchAllAssociated("2", INDEX_NAME));
		Assertions.assertEquals(2, verifyResult.getAssociatedDocumentCount());

		// Batch fetch with associated documents (all associated files)
		BatchFetchGroupBuilder assocGroup = new BatchFetchGroupBuilder(INDEX_NAME, List.of("1", "2")).setAssociatedFetchType(FetchType.FULL)
				.setRealtime(true);
		BatchFetch assocFetch = new BatchFetch().addFetchGroup(assocGroup);
		BatchFetchResult assocResult = zuliaWorkPool.batchFetch(assocFetch);
		List<FetchResult> assocResults = assocResult.getFetchResults();
		Assertions.assertEquals(2, assocResults.size());

		// Doc 1 has 1 associated file, doc 2 has 2
		int totalAssocDocs = assocResults.stream().mapToInt(FetchResult::getAssociatedDocumentCount).sum();
		Assertions.assertEquals(3, totalAssocDocs);

		for (FetchResult fr : assocResults) {
			Assertions.assertTrue(fr.hasResultDocument());
			Assertions.assertTrue(fr.getAssociatedDocumentCount() > 0);
			for (AssociatedResult ar : fr.getAssociatedDocuments()) {
				Assertions.assertNotNull(ar.getFilename());
				Assertions.assertTrue(ar.hasDocument());
				Assertions.assertFalse(ar.getDocumentAsUtf8().isEmpty());
			}
		}

		// Batch fetch associated by specific filename
		BatchFetchGroupBuilder fileGroup = new BatchFetchGroupBuilder(INDEX_NAME, List.of("1", "2")).setAssociatedFetchType(FetchType.FULL)
				.setFilename("notes.txt").setRealtime(true);
		BatchFetch fileFetch = new BatchFetch().addFetchGroup(fileGroup);
		BatchFetchResult fileResult = zuliaWorkPool.batchFetch(fileFetch);
		List<FetchResult> fileResults = fileResult.getFetchResults();
		Assertions.assertEquals(2, fileResults.size());
		for (FetchResult fr : fileResults) {
			Assertions.assertEquals(1, fr.getAssociatedDocumentCount());
			Assertions.assertEquals("notes.txt", fr.getFirstAssociatedDocument().getFilename());
		}

		// Batch fetch with NONE result type + FULL associated (associated docs only)
		BatchFetchGroupBuilder assocOnlyGroup = new BatchFetchGroupBuilder(INDEX_NAME, List.of("1", "2")).setResultFetchType(FetchType.NONE)
				.setAssociatedFetchType(FetchType.FULL).setRealtime(true);
		BatchFetch assocOnlyFetch = new BatchFetch().addFetchGroup(assocOnlyGroup);
		BatchFetchResult assocOnlyResult = zuliaWorkPool.batchFetch(assocOnlyFetch);
		List<FetchResult> assocOnlyResults = assocOnlyResult.getFetchResults();
		Assertions.assertEquals(2, assocOnlyResults.size());
		for (FetchResult fr : assocOnlyResults) {
			Assertions.assertFalse(fr.hasResultDocument(), "result document should not be fetched");
			Assertions.assertTrue(fr.getAssociatedDocumentCount() > 0, "associated docs should be present");
		}

		// Doc without associated files should return empty associated list
		BatchFetchGroupBuilder noAssocGroup = new BatchFetchGroupBuilder(INDEX_NAME, List.of("3")).setAssociatedFetchType(FetchType.FULL).setRealtime(true);
		BatchFetch noAssocFetch = new BatchFetch().addFetchGroup(noAssocGroup);
		BatchFetchResult noAssocResult = zuliaWorkPool.batchFetch(noAssocFetch);
		List<FetchResult> noAssocResults = noAssocResult.getFetchResults();
		Assertions.assertEquals(1, noAssocResults.size());
		Assertions.assertTrue(noAssocResults.getFirst().hasResultDocument());
		Assertions.assertEquals(0, noAssocResults.getFirst().getAssociatedDocumentCount());
	}

	@Test
	@Order(6)
	public void batchDeleteTest() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// Index extra documents to delete
		indexRecord(100, "To Be Deleted 1", "temp", 1.0);
		indexRecord(101, "To Be Deleted 2", "temp", 1.0);
		indexRecord(102, "To Be Deleted 3", "temp", 1.0);

		Search search = new Search(INDEX_NAME).setRealtime(true);
		SearchResult searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(UNIQUE_DOCS + 3, searchResult.getTotalHits());

		// Batch delete specific IDs
		BatchDelete batchDelete = new BatchDelete();
		batchDelete.addDelete(new DeleteFromIndex("100", INDEX_NAME));
		batchDelete.addDelete(new DeleteFromIndex("101", INDEX_NAME));
		batchDelete.addDelete(new DeleteFromIndex("102", INDEX_NAME));
		zuliaWorkPool.batchDelete(batchDelete);

		// Batch delete from search results
		indexRecord(200, "Delete via search 1", "temp2", 1.0);
		indexRecord(201, "Delete via search 2", "temp2", 1.0);

		search = new Search(INDEX_NAME).setAmount(10).setRealtime(true);
		search.addQuery(new FilterQuery("category:temp2"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(2, searchResult.getTotalHits());

		BatchDelete deleteFromResults = new BatchDelete().deleteDocumentFromQueryResult(searchResult);
		zuliaWorkPool.batchDelete(deleteFromResults);

		// Verify the original documents remain after all batch deletes
		search = new Search(INDEX_NAME).setRealtime(true);
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(UNIQUE_DOCS, searchResult.getTotalHits());
	}

	@Test
	@Order(7)
	public void clearIndexTest() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// Verify documents exist
		Search search = new Search(INDEX_NAME).setRealtime(true);
		SearchResult searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(UNIQUE_DOCS, searchResult.getTotalHits());

		// Clear the index
		zuliaWorkPool.clearIndex(INDEX_NAME);

		// Verify all documents are gone
		search = new Search(INDEX_NAME).setRealtime(true);
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(0, searchResult.getTotalHits());

		// Index config should still exist
		Assertions.assertNotNull(zuliaWorkPool.getIndexConfig(INDEX_NAME));
	}

	@Test
	@Order(8)
	public void optimizeAndReindexTest() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// Re-add documents after clear
		indexRecord(1, "Java Programming", "tech", 4.5);
		indexRecord(2, "Python Basics", "tech", 3.8);
		indexRecord(3, "Cooking Guide", "food", 4.2);

		Search search = new Search(INDEX_NAME).setRealtime(true);
		SearchResult searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(3, searchResult.getTotalHits());

		// Optimize the index
		zuliaWorkPool.optimizeIndex(INDEX_NAME);

		// Documents should still be searchable after optimize
		search = new Search(INDEX_NAME).setRealtime(true);
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(3, searchResult.getTotalHits());

		// Reindex
		zuliaWorkPool.reindex(INDEX_NAME);

		// Documents should still be searchable after reindex
		search = new Search(INDEX_NAME).setRealtime(true);
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(3, searchResult.getTotalHits());
	}

	@Test
	@Order(9)
	public void restart() throws Exception {
		nodeExtension.restartNodes();
	}

	@Test
	@Order(10)
	public void confirm() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// After restart, documents should persist
		Search search = new Search(INDEX_NAME);
		SearchResult searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(3, searchResult.getTotalHits());

		// Field filtering should still work
		Fetch fetch = new Fetch("1", INDEX_NAME);
		fetch.addDocumentField("title");
		FetchResult fetchResult = zuliaWorkPool.fetch(fetch);
		Document doc = fetchResult.getDocument();
		Assertions.assertNotNull(doc.getString("title"));
		Assertions.assertNull(doc.getString("category"));
	}

}
