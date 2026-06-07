package io.zulia.server.test.node;

import io.zulia.DefaultAnalyzers;
import io.zulia.client.command.Store;
import io.zulia.client.command.builder.CountFacet;
import io.zulia.client.command.builder.FilterQuery;
import io.zulia.client.command.builder.Search;
import io.zulia.client.command.builder.Sort;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.SearchResult;
import io.zulia.doc.ResultDocBuilder;
import io.zulia.fields.FieldConfigBuilder;
import io.zulia.message.ZuliaIndex.FacetAs;
import io.zulia.message.ZuliaQuery.FacetCount;
import io.zulia.server.test.node.shared.NodeExtension;
import io.zulia.util.ZuliaDateUtil;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Verifies that a DATE field accepts date values supplied as Strings (e.g. "2024-06-17T16:10:00Z", "2024-06-17",
 * an offset-less "2024-03-15T08:30:00", or a year/year-month) as well as epoch milliseconds as a Number, across all
 * paths that consume a date: indexing (range queries), sorting, and faceting (flat and hierarchical). Blank values
 * are skipped like absent fields, and unparseable strings are rejected with a message listing every supported format.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class DateStringTest {

	@RegisterExtension
	static final NodeExtension nodeExtension = new NodeExtension(3);

	private static final String INDEX_NAME = "dateStringTest";

	// real Date used for the control document, also the latest date in the set
	private static final Date CONTROL_DATE = Date.from(LocalDate.of(2025, Month.DECEMBER, 25).atStartOfDay(ZoneId.of("UTC")).toInstant());

	// epoch milliseconds (a long) for 2023-07-04, supplied as a Number rather than a String
	private static final long EPOCH_MILLIS_2023 = Date.from(LocalDate.of(2023, Month.JULY, 4).atStartOfDay(ZoneId.of("UTC")).toInstant()).getTime();

	@Test
	@Order(1)
	public void createIndexAndStore() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("id");
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		// one DATE field exercised for indexing, sorting, a flat facet ("added") and a hierarchical facet ("addedHier")
		indexConfig.addFieldConfig(FieldConfigBuilder.createDate("added").index().sort().facetAs(FacetAs.DateHandling.DATE_YYYY_MM_DD)
				.facetAsHierarchical("addedHier", FacetAs.DateHandling.DATE_YYYY_MM_DD));
		indexConfig.setIndexName(INDEX_NAME);
		indexConfig.setNumberOfShards(1);
		indexConfig.setShardCommitInterval(2); //force some commits

		zuliaWorkPool.createIndex(indexConfig);

		// added supplied in every supported form
		store(zuliaWorkPool, "0", "2024-06-17T16:10:00Z");  // full timestamp with offset
		store(zuliaWorkPool, "1", "2024-06-17");            // date only -> start of day UTC
		store(zuliaWorkPool, "2", "2024/06/18");            // slash separator, date only
		store(zuliaWorkPool, "3", "2022");                  // year only -> 2022-01-01
		store(zuliaWorkPool, "4", "2022-03");               // year-month -> 2022-03-01
		store(zuliaWorkPool, "5", CONTROL_DATE);            // real java.util.Date control
		store(zuliaWorkPool, "6", "");                      // blank -> skipped (no date indexed)
		store(zuliaWorkPool, "7", "2024-03-15T08:30:00");   // timestamp without offset -> assumed UTC
		store(zuliaWorkPool, "8", EPOCH_MILLIS_2023);       // epoch milliseconds as a Number
	}

	@Test
	@Order(2)
	public void rangeQuery() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// all 9 docs are stored, including the blank one (its date is simply not indexed)
		SearchResult all = zuliaWorkPool.search(new Search(INDEX_NAME).setAmount(100).setRealtime(true));
		Assertions.assertEquals(9, all.getTotalHits());

		// the timestamp doc (id 0) and the date-only doc (id 1) both fall on 2024-06-17
		SearchResult onDay = zuliaWorkPool.search(new Search(INDEX_NAME).setAmount(100).setRealtime(true).addQuery(new FilterQuery("added:2024-06-17")));
		Assertions.assertEquals(2, onDay.getTotalHits());

		// a year query covers the whole year: 2024-06-17 (x2), 2024-06-18, and the offset-less 2024-03-15
		SearchResult onYear = zuliaWorkPool.search(new Search(INDEX_NAME).setAmount(100).setRealtime(true).addQuery(new FilterQuery("added:2024")));
		Assertions.assertEquals(4, onYear.getTotalHits());

		// explicit range with a full timestamp upper bound matches every parsed form within it
		SearchResult range = zuliaWorkPool.search(
				new Search(INDEX_NAME).setAmount(100).setRealtime(true).addQuery(new FilterQuery("added:[2022-02-01 TO 2024-06-17T23:59:59Z]")));
		Assertions.assertEquals(5, range.getTotalHits()); // 2022-03-01, 2023-07-04, 2024-03-15, 2024-06-17 (x2)
	}

	@Test
	@Order(3)
	public void sort() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		Search ascending = new Search(INDEX_NAME).setAmount(10).setRealtime(true);
		ascending.addSort(new Sort("added").ascending().missingLast());
		Assertions.assertEquals("3", zuliaWorkPool.search(ascending).getFirstDocument().get("id")); // 2022-01-01 is earliest

		// missingFirst ranks the missing value lowest, so in descending order the latest real date comes first
		Search descending = new Search(INDEX_NAME).setAmount(10).setRealtime(true);
		descending.addSort(new Sort("added").descending().missingFirst());
		Assertions.assertEquals("5", zuliaWorkPool.search(descending).getFirstDocument().get("id")); // control 2025-12-25 is latest

		// the blank doc has no sort value, so it lands last when missing values sort last
		Search ascendingMissingLast = new Search(INDEX_NAME).setAmount(10).setRealtime(true);
		ascendingMissingLast.addSort(new Sort("added").ascending().missingLast());
		Assertions.assertEquals("6", zuliaWorkPool.search(ascendingMissingLast).getDocuments().getLast().get("id"));
	}

	@Test
	@Order(4)
	public void flatFacet() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		SearchResult facetResult = zuliaWorkPool.search(
				new Search(INDEX_NAME).setAmount(0).setRealtime(true).addCountFacet(new CountFacet("added").setTopN(20)));

		Map<String, Long> counts = facetCounts(facetResult, "added");

		Assertions.assertEquals(2L, (long) counts.get("2024-06-17")); // id 0 (timestamp) + id 1 (date)
		Assertions.assertEquals(1L, (long) counts.get("2024-06-18")); // id 2 (slash separator)
		Assertions.assertEquals(1L, (long) counts.get("2022-01-01")); // id 3 (year only)
		Assertions.assertEquals(1L, (long) counts.get("2022-03-01")); // id 4 (year-month)
		Assertions.assertEquals(1L, (long) counts.get("2025-12-25")); // id 5 (control Date)
		Assertions.assertEquals(1L, (long) counts.get("2024-03-15")); // id 7 (offset-less timestamp)
		Assertions.assertEquals(1L, (long) counts.get("2023-07-04")); // id 8 (epoch millis Number)
		Assertions.assertNull(counts.get("1970-01-01")); // blank (id 6) produced no facet
	}

	@Test
	@Order(5)
	public void hierarchicalFacet() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// top level of a hierarchical date facet is the year
		SearchResult facetResult = zuliaWorkPool.search(
				new Search(INDEX_NAME).setAmount(0).setRealtime(true).addCountFacet(new CountFacet("addedHier").setTopN(20)));

		Map<String, Long> yearCounts = facetCounts(facetResult, "addedHier");

		Assertions.assertEquals(4L, (long) yearCounts.get("2024")); // id 0, 1, 2, 7
		Assertions.assertEquals(2L, (long) yearCounts.get("2022")); // id 3, 4
		Assertions.assertEquals(1L, (long) yearCounts.get("2025")); // id 5
		Assertions.assertEquals(1L, (long) yearCounts.get("2023")); // id 8 (epoch millis Number)

		// drill into 2024 -> month 3 (offset-less timestamp) and month 6 (the rest)
		SearchResult drill = zuliaWorkPool.search(new Search(INDEX_NAME).setAmount(0).setRealtime(true).addCountFacet(new CountFacet("addedHier", "2024")));
		Map<String, Long> monthCounts = new HashMap<>();
		for (FacetCount facetCount : drill.getFacetCountsForPath("addedHier", "2024")) {
			monthCounts.put(facetCount.getFacet(), facetCount.getCount());
		}
		Assertions.assertEquals(3L, (long) monthCounts.get("6")); // id 0, 1, 2
		Assertions.assertEquals(1L, (long) monthCounts.get("3")); // id 7
	}

	@Test
	@Order(6)
	public void invalidStringRejected() {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		Document badDocument = new Document();
		badDocument.put("id", "bad");
		badDocument.put("added", "not-a-date");

		// unparseable date string fails the store (the sort path validates first, then indexing)
		Assertions.assertThrows(Exception.class, () -> zuliaWorkPool.store(new Store("bad", INDEX_NAME, ResultDocBuilder.from(badDocument))));
	}

	@Test
	@Order(7)
	public void convertToDateContract() {
		// null and blank are treated as no value (skipped), not errors
		Assertions.assertNull(ZuliaDateUtil.convertToDate(null, "field <added>"));
		Assertions.assertNull(ZuliaDateUtil.convertToDate("", "field <added>"));
		Assertions.assertNull(ZuliaDateUtil.convertToDate("   ", "field <added>"));

		// Date passes through unchanged
		Date real = new Date(1_700_000_000_000L);
		Assertions.assertSame(real, ZuliaDateUtil.convertToDate(real, "field <added>"));

		// a Number is interpreted as epoch milliseconds
		Assertions.assertEquals(new Date(1_700_000_000_000L), ZuliaDateUtil.convertToDate(1_700_000_000_000L, "field <added>"));

		// an offset-less timestamp is assumed to be UTC
		Assertions.assertEquals(Date.from(LocalDateTime.of(2024, 3, 15, 8, 30, 0).toInstant(ZoneOffset.UTC)),
				ZuliaDateUtil.convertToDate("2024-03-15T08:30:00", "field <added>"));

		// an explicit offset is honored rather than overridden to UTC
		Assertions.assertEquals(Date.from(LocalDateTime.of(2024, 3, 15, 8, 30, 0).toInstant(ZoneOffset.ofHoursMinutes(5, 30))),
				ZuliaDateUtil.convertToDate("2024-03-15T08:30:00+05:30", "field <added>"));

		// an unparseable string is rejected with a message listing every supported format
		IllegalArgumentException badString = Assertions.assertThrows(IllegalArgumentException.class,
				() -> ZuliaDateUtil.convertToDate("not-a-date", "field <added>"));
		Assertions.assertTrue(badString.getMessage().contains("cannot be parsed"), badString.getMessage());
		Assertions.assertTrue(badString.getMessage().contains(ZuliaDateUtil.SUPPORTED_DATE_STRING_FORMATS), badString.getMessage());

		// an unsupported type is rejected
		IllegalArgumentException badType = Assertions.assertThrows(IllegalArgumentException.class,
				() -> ZuliaDateUtil.convertToDate(List.of(1, 2), "field <added>"));
		Assertions.assertTrue(badType.getMessage().contains("Expecting Date"), badType.getMessage());
	}

	private static Map<String, Long> facetCounts(SearchResult searchResult, String field) {
		Map<String, Long> counts = new HashMap<>();
		for (FacetCount facetCount : searchResult.getFacetCounts(field)) {
			counts.put(facetCount.getFacet(), facetCount.getCount());
		}
		return counts;
	}

	private static void store(ZuliaWorkPool zuliaWorkPool, String id, Object added) throws Exception {
		Document document = new Document();
		document.put("id", id);
		document.put("added", added);
		zuliaWorkPool.store(new Store(id, INDEX_NAME, ResultDocBuilder.from(document)));
	}
}
