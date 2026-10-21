package io.zulia.server.test.node;

import io.zulia.DefaultAnalyzers;
import io.zulia.client.command.Store;
import io.zulia.client.command.builder.CountDistinct;
import io.zulia.client.command.builder.FilterQuery;
import io.zulia.client.command.builder.Search;
import io.zulia.client.command.builder.Sort;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.rest.ZuliaRESTClient;
import io.zulia.client.rest.options.SearchREST;
import io.zulia.client.result.SearchResult;
import io.zulia.doc.ResultDocBuilder;
import io.zulia.fields.FieldConfigBuilder;
import io.zulia.message.ZuliaQuery.DistinctCount;
import io.zulia.rest.dto.DistinctCountDTO;
import io.zulia.rest.dto.SearchResultsDTO;
import io.zulia.server.test.node.shared.RestNodeExtension;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class DistinctCountTest {

	@RegisterExtension
	static final RestNodeExtension nodeExtension = new RestNodeExtension(3);

	private static final String MULTI_SHARD_INDEX = "distinctCountMulti";
	private static final String SINGLE_SHARD_INDEX = "distinctCountSingle";
	private static final int JOURNALS = 25;
	private static final int ORPHANS = 5;

	/**
	 * journal and journalId are null for an article with no journal.
	 */
	private record Article(String id, String journal, Integer journalId, int year, String parity) {
	}

	private static final List<Article> ARTICLES = buildArticles();

	private static List<Article> buildArticles() {
		List<Article> articles = new ArrayList<>();
		for (int j = 0; j < JOURNALS; j++) {
			int articleCount = (j % 5) + 1;
			for (int a = 0; a < articleCount; a++) {
				String parity = j % 2 == 0 ? "even" : "odd";
				articles.add(new Article("j" + j + "a" + a, "JOURNAL" + j, j, 2015 + a, parity));
			}
		}
		for (int o = 0; o < ORPHANS; o++) {
			articles.add(new Article("orphan" + o, null, null, 2030, "orphan"));
		}
		return articles;
	}

	private static long expectedDistinctJournals(Predicate<Article> filter) {
		return ARTICLES.stream().filter(filter).map(Article::journal).filter(Objects::nonNull).distinct().count();
	}

	private static long expectedDistinctYears(Predicate<Article> filter) {
		return ARTICLES.stream().filter(filter).map(Article::year).distinct().count();
	}

	private static long expectedHits(Predicate<Article> filter) {
		return ARTICLES.stream().filter(filter).count();
	}

	@Test
	@Order(1)
	public void createIndexes() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getGrpcClient();
		zuliaWorkPool.createIndex(indexConfig(MULTI_SHARD_INDEX, 3));
		zuliaWorkPool.createIndex(indexConfig(SINGLE_SHARD_INDEX, 1));
	}

	private static ClientIndexConfig indexConfig(String indexName, int shards) {
		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("title");
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("title").indexAs(DefaultAnalyzers.STANDARD));
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("journal").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createInt("journalId").index().sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createInt("year").index().sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createGeoPoint("location").index().sort());
		indexConfig.setIndexName(indexName);
		indexConfig.setNumberOfShards(shards);
		indexConfig.setShardCommitInterval(20); // force several segments so ordinals span leaves
		return indexConfig;
	}

	@Test
	@Order(2)
	public void index() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getGrpcClient();
		for (Article article : ARTICLES) {
			Document document = new Document();
			document.put("id", article.id());
			document.put("title", "article " + article.id() + " " + article.parity());
			if (article.journal() != null) {
				document.put("journal", article.journal());
				document.put("journalId", article.journalId());
			}
			document.put("year", article.year());

			for (String index : List.of(MULTI_SHARD_INDEX, SINGLE_SHARD_INDEX)) {
				Store store = new Store(article.id(), index);
				store.setResultDocument(ResultDocBuilder.newBuilder().setDocument(document));
				zuliaWorkPool.store(store);
			}
		}

		// a realtime search commits every shard so the later searches see all the documents
		for (String index : List.of(MULTI_SHARD_INDEX, SINGLE_SHARD_INDEX)) {
			SearchResult visible = zuliaWorkPool.search(new Search(index).setAmount(0).setRealtime(true));
			Assertions.assertEquals(ARTICLES.size(), visible.getTotalHits());
		}
	}

	@Test
	@Order(3)
	public void countWithoutChangingHits() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getGrpcClient();

		Search plain = new Search(MULTI_SHARD_INDEX).setAmount(10).addSort(new Sort("id"));
		SearchResult plainResult = zuliaWorkPool.search(plain);

		Search counted = new Search(MULTI_SHARD_INDEX).setAmount(10).addSort(new Sort("id")).addDistinctCount("journal");
		SearchResult countedResult = zuliaWorkPool.search(counted);

		Assertions.assertEquals(expectedHits(a -> true), plainResult.getTotalHits());
		Assertions.assertEquals(plainResult.getTotalHits(), countedResult.getTotalHits());
		Assertions.assertEquals(plainResult.getUniqueIds(), countedResult.getUniqueIds());
		Assertions.assertTrue(plainResult.getDistinctCounts().isEmpty());

		DistinctCount journals = countedResult.getDistinctCount("journal");
		Assertions.assertNotNull(journals);
		Assertions.assertEquals(JOURNALS, journals.getCount());
		Assertions.assertTrue(journals.getExact());
		Assertions.assertNull(countedResult.getDistinctCount("year"));
	}

	@Test
	@Order(4)
	public void filteredCountsMatchTheData() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getGrpcClient();

		for (String parity : List.of("even", "odd")) {
			Predicate<Article> filter = a -> a.parity().equals(parity);
			Search search = new Search(MULTI_SHARD_INDEX).setAmount(0).addQuery(new FilterQuery("title:" + parity));
			search.addDistinctCount("journal").addDistinctCount("journalId").addDistinctCount("year");
			SearchResult result = zuliaWorkPool.search(search);

			Assertions.assertEquals(expectedHits(filter), result.getTotalHits());
			Assertions.assertEquals(List.of("journal", "journalId", "year"), result.getDistinctCounts().stream().map(DistinctCount::getField).toList());
			Assertions.assertEquals(expectedDistinctJournals(filter), result.getDistinctCount("journal").getCount());
			Assertions.assertEquals(expectedDistinctJournals(filter), result.getDistinctCount("journalId").getCount());
			Assertions.assertEquals(expectedDistinctYears(filter), result.getDistinctCount("year").getCount());
			result.getDistinctCounts().forEach(dc -> Assertions.assertTrue(dc.getExact(), dc.getField() + " should be exact"));
		}
	}

	@Test
	@Order(5)
	public void missingValuesAreNotCounted() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getGrpcClient();

		Search search = new Search(MULTI_SHARD_INDEX).setAmount(0).addQuery(new FilterQuery("title:orphan")).addDistinctCount("journal")
				.addDistinctCount("year");
		SearchResult result = zuliaWorkPool.search(search);

		Assertions.assertEquals(ORPHANS, result.getTotalHits());
		Assertions.assertEquals(0, result.getDistinctCount("journal").getCount());
		Assertions.assertTrue(result.getDistinctCount("journal").getExact());
		Assertions.assertEquals(1, result.getDistinctCount("year").getCount());
	}

	@Test
	@Order(6)
	public void singleShardMatchesMultiShard() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getGrpcClient();

		for (String query : List.of("*:*", "title:even", "title:odd", "title:orphan")) {
			Search multi = new Search(MULTI_SHARD_INDEX).setAmount(0).addQuery(new FilterQuery(query)).addDistinctCount("journal").addDistinctCount("year");
			Search single = new Search(SINGLE_SHARD_INDEX).setAmount(0).addQuery(new FilterQuery(query)).addDistinctCount("journal").addDistinctCount("year");
			SearchResult multiResult = zuliaWorkPool.search(multi);
			SearchResult singleResult = zuliaWorkPool.search(single);
			Assertions.assertEquals(singleResult.getDistinctCounts(), multiResult.getDistinctCounts(), query);
		}

		Search both = new Search(MULTI_SHARD_INDEX, SINGLE_SHARD_INDEX).setAmount(0).addDistinctCount("journal");
		SearchResult bothResult = zuliaWorkPool.search(both);
		Assertions.assertEquals(2 * expectedHits(a -> true), bothResult.getTotalHits());
		Assertions.assertEquals(JOURNALS, bothResult.getDistinctCount("journal").getCount(), "the same journals in two indexes are still the same journals");
		Assertions.assertTrue(bothResult.getDistinctCount("journal").getExact());
	}

	@Test
	@Order(7)
	public void cappedCountIsAFloor() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getGrpcClient();

		Search search = new Search(MULTI_SHARD_INDEX).setAmount(0).addDistinctCount(new CountDistinct("journal").setMaxGroupsPerShard(3));
		SearchResult result = zuliaWorkPool.search(search);

		DistinctCount journals = result.getDistinctCount("journal");
		Assertions.assertFalse(journals.getExact());
		Assertions.assertTrue(journals.getCount() >= 1 && journals.getCount() <= JOURNALS, "floor was " + journals.getCount());

		// id routing can spread the five years thinly over the shards, so cap at one to be sure at least one shard crosses it
		Search cappedYears = new Search(MULTI_SHARD_INDEX).setAmount(0).addQuery(new FilterQuery("title:odd"))
				.addDistinctCount(new CountDistinct("year").setMaxGroupsPerShard(1));
		DistinctCount years = zuliaWorkPool.search(cappedYears).getDistinctCount("year");
		Assertions.assertFalse(years.getExact(), "a cap of 1 per shard is below the 5 years present: " + years);
		Assertions.assertTrue(years.getCount() >= 1 && years.getCount() <= 5, "floor was " + years.getCount());

		Search roomy = new Search(MULTI_SHARD_INDEX).setAmount(0).addDistinctCount(new CountDistinct("journal").setMaxGroupsPerShard(JOURNALS));
		DistinctCount roomyJournals = zuliaWorkPool.search(roomy).getDistinctCount("journal");
		Assertions.assertTrue(roomyJournals.getExact());
		Assertions.assertEquals(JOURNALS, roomyJournals.getCount());
	}

	@Test
	@Order(8)
	public void everyPageReportsTheSameCount() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getGrpcClient();

		Search search = new Search(MULTI_SHARD_INDEX).setAmount(7).addSort(new Sort("id")).addDistinctCount("journal");
		AtomicInteger pages = new AtomicInteger();
		AtomicInteger hits = new AtomicInteger();
		zuliaWorkPool.searchAll(search, page -> {
			pages.incrementAndGet();
			hits.addAndGet(page.getCompleteResults().size());
			Assertions.assertEquals(JOURNALS, page.getDistinctCount("journal").getCount());
			Assertions.assertTrue(page.getDistinctCount("journal").getExact());
		});
		Assertions.assertEquals(expectedHits(a -> true), hits.get());
		Assertions.assertTrue(pages.get() > 1);
	}

	@Test
	@Order(9)
	public void cachedAndUncachedAgree() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getGrpcClient();

		Search cached = new Search(MULTI_SHARD_INDEX).setAmount(0).addQuery(new FilterQuery("title:even")).addDistinctCount("journal").setDontCache(false);
		SearchResult first = zuliaWorkPool.search(cached);
		SearchResult second = zuliaWorkPool.search(cached);
		Assertions.assertEquals(first.getDistinctCounts(), second.getDistinctCounts());
		Assertions.assertEquals(expectedDistinctJournals(a -> a.parity().equals("even")), second.getDistinctCount("journal").getCount());

		Search plainCached = new Search(MULTI_SHARD_INDEX).setAmount(0).addQuery(new FilterQuery("title:even")).setDontCache(false);
		Assertions.assertTrue(zuliaWorkPool.search(plainCached).getDistinctCounts().isEmpty(), "a counted query must not share a cache entry with a plain one");
	}

	@Test
	@Order(12)
	public void concurrentSearchesAgreeWithoutTheCache() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getGrpcClient();
		List<String> filters = List.of("", "title:even", "title:odd", "title:orphan");
		List<Predicate<Article>> predicates = List.of(a -> true, a -> a.parity().equals("even"), a -> a.parity().equals("odd"),
				a -> a.parity().equals("orphan"));
		List<Future<Void>> futures = new ArrayList<>();
		try (ExecutorService clients = Executors.newFixedThreadPool(16)) {
			for (int i = 0; i < 96; i++) {
				int which = i % filters.size();
				futures.add(clients.submit(() -> {
					Search search = new Search(MULTI_SHARD_INDEX, SINGLE_SHARD_INDEX).setAmount(0).setDontCache(true).addDistinctCount("journal")
							.addDistinctCount("year");
					if (!filters.get(which).isEmpty()) {
						search.addQuery(new FilterQuery(filters.get(which)));
					}
					SearchResult result = zuliaWorkPool.search(search);
					Assertions.assertEquals(expectedDistinctJournals(predicates.get(which)), result.getDistinctCount("journal").getCount(), filters.get(which));
					Assertions.assertEquals(expectedDistinctYears(predicates.get(which)), result.getDistinctCount("year").getCount(), filters.get(which));
					Assertions.assertTrue(result.getDistinctCount("journal").getExact());
					return null;
				}));
			}
			for (Future<Void> future : futures) {
				future.get();
			}
		}
	}

	@Test
	@Order(10)
	public void badRequestsAreRejected() {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getGrpcClient();

		Search notSortable = new Search(MULTI_SHARD_INDEX).setAmount(0).addDistinctCount("title");
		Assertions.assertThrows(Exception.class, () -> zuliaWorkPool.search(notSortable), "title is not sortable");

		Search unknown = new Search(MULTI_SHARD_INDEX).setAmount(0).addDistinctCount("madeUp");
		Assertions.assertThrows(Exception.class, () -> zuliaWorkPool.search(unknown), "madeUp does not exist");

		Search score = new Search(MULTI_SHARD_INDEX).setAmount(0).addDistinctCount("zuliaScore");
		Exception scoreError = Assertions.assertThrows(Exception.class, () -> zuliaWorkPool.search(score), "the score is sortable but computed per query");
		Assertions.assertTrue(scoreError.getMessage().contains("zuliaScore") && scoreError.getMessage().contains("computed per query"),
				scoreError.getMessage());

		Search geo = new Search(MULTI_SHARD_INDEX).setAmount(0).addDistinctCount("location");
		Exception geoError = Assertions.assertThrows(Exception.class, () -> zuliaWorkPool.search(geo), "a geo point sorts by one coordinate");
		Assertions.assertTrue(geoError.getMessage().contains("<location>") && geoError.getMessage().contains("GEO_POINT"), geoError.getMessage());

		Search repeated = new Search(MULTI_SHARD_INDEX).setAmount(0).addDistinctCount("journal").addDistinctCount("journal");
		Assertions.assertThrows(Exception.class, () -> zuliaWorkPool.search(repeated), "journal is requested twice");

		Search empty = new Search(MULTI_SHARD_INDEX).setAmount(0).addDistinctCount("");
		Assertions.assertThrows(Exception.class, () -> zuliaWorkPool.search(empty), "field is required");

		IllegalArgumentException negative = Assertions.assertThrows(IllegalArgumentException.class, () -> new CountDistinct("journal").setMaxGroupsPerShard(-1),
				"the client rejects a negative cap before it wraps");
		Assertions.assertTrue(negative.getMessage().contains("<journal>") && negative.getMessage().contains("-1"), negative.getMessage());

		Search tooMany = new Search(MULTI_SHARD_INDEX).setAmount(0).addDistinctCount(new CountDistinct("journal").setMaxGroupsPerShard(9_000_000))
				.addDistinctCount(new CountDistinct("year").setMaxGroupsPerShard(9_000_000));
		Exception e = Assertions.assertThrows(Exception.class, () -> zuliaWorkPool.search(tooMany), "caps across fields exceed the per shard limit");
		Assertions.assertTrue(e.getMessage().contains("18000000"), e.getMessage());
	}

	@Test
	@Order(11)
	public void restSurfacesTheCounts() {
		ZuliaRESTClient restClient = nodeExtension.getRESTClient();

		SearchResultsDTO results = restClient.search(new SearchREST(MULTI_SHARD_INDEX).setQuery("title:odd").setDistinctCount("journal", "year:1"));

		Predicate<Article> odd = a -> a.parity().equals("odd");
		Assertions.assertEquals(expectedHits(odd), results.getTotalHits());
		List<DistinctCountDTO> distinctCounts = results.getDistinctCounts();
		Assertions.assertEquals(2, distinctCounts.size());
		Assertions.assertEquals(new DistinctCountDTO("journal", expectedDistinctJournals(odd), true), distinctCounts.get(0));
		Assertions.assertEquals("year", distinctCounts.get(1).field());
		Assertions.assertFalse(distinctCounts.get(1).exact(), "a cap of 1 per shard is below the 5 years present: " + distinctCounts);

		SearchResultsDTO plain = restClient.search(new SearchREST(MULTI_SHARD_INDEX).setQuery("title:odd"));
		Assertions.assertNull(plain.getDistinctCounts());
	}
}
