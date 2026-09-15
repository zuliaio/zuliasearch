package io.zulia.server.test.node;

import io.zulia.DefaultAnalyzers;
import io.zulia.client.command.Fetch;
import io.zulia.client.command.Store;
import io.zulia.client.command.UpdateIndex;
import io.zulia.client.command.builder.CountFacet;
import io.zulia.client.command.builder.FilterQuery;
import io.zulia.client.command.builder.NumericStat;
import io.zulia.client.command.builder.Search;
import io.zulia.client.command.builder.Sort;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.SearchResult;
import io.zulia.doc.ResultDocBuilder;
import io.zulia.fields.FieldConfigBuilder;
import io.zulia.message.ZuliaIndex.DefaultValue;
import io.zulia.message.ZuliaIndex.FacetAs;
import io.zulia.message.ZuliaIndex.FieldConfig;
import io.zulia.message.ZuliaIndex.GeoPointValue;
import io.zulia.message.ZuliaIndex.IndexAs;
import io.zulia.message.ZuliaQuery;
import io.zulia.server.test.node.shared.NodeExtension;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Defaults feed the index, sort and facet paths when the stored path resolves to nothing, and the stored document is
 * never modified. The exists marker is withheld when every value was defaulted.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class FieldDefaultValueTest {

	@RegisterExtension
	static final NodeExtension nodeExtension = new NodeExtension(1);

	public static final String INDEX_NAME = "fieldDefaultValueTest";
	public static final String UPDATE_INDEX_NAME = "fieldDefaultUpdateTest";

	@Test
	@Order(1)
	public void createIndex() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("title");
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("title").indexAs(DefaultAnalyzers.STANDARD));
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("journal").indexAs(DefaultAnalyzers.LC_KEYWORD).facet().sort().defaultValue("unknown"));
		indexConfig.addFieldConfig(FieldConfigBuilder.createInt("citationCount").index().facet().sort().defaultValue(0));
		indexConfig.addFieldConfig(FieldConfigBuilder.createLong("bytes").index().sort().defaultValue(0L));
		indexConfig.addFieldConfig(FieldConfigBuilder.createFloat("score").index().sort().defaultValue(0.0f));
		indexConfig.addFieldConfig(FieldConfigBuilder.createDouble("weight").index().sort().defaultValue(1.0d));
		indexConfig.addFieldConfig(FieldConfigBuilder.createBool("active").index().facet().sort().defaultValue(false));
		indexConfig.addFieldConfig(
				FieldConfigBuilder.createDate("pubDate").index().facetAs(FacetAs.DateHandling.DATE_YYYY_MM_DD).sort().defaultValue(Date.from(Instant.EPOCH)));
		indexConfig.addFieldConfig(
				FieldConfigBuilder.createString("authors.affiliation").indexAs(DefaultAnalyzers.LC_KEYWORD, "affiliation").facetAs("affiliation")
						.defaultValue("unknown", DefaultValue.Fill.EACH_ELEMENT));
		indexConfig.addFieldConfig(
				FieldConfigBuilder.createString("tags").indexAs(DefaultAnalyzers.LC_KEYWORD).facet().defaultValue("untagged", DefaultValue.Fill.EACH_ELEMENT));
		// control field without a default
		indexConfig.addFieldConfig(FieldConfigBuilder.createInt("noDefault").index().sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createGeoPoint("location").index().sort().defaultValue(38.9072, -77.0369));
		indexConfig.setIndexName(INDEX_NAME);
		indexConfig.setNumberOfShards(1);

		zuliaWorkPool.createIndex(indexConfig);
	}

	@Test
	@Order(2)
	public void index() throws Exception {
		Document full = new Document().append("journal", "Nature").append("citationCount", 5).append("bytes", 10L).append("score", 2.5f).append("weight", 3.0d)
				.append("active", true).append("pubDate", "2024-06-17").append("authors", List.of(author("A", "MIT"), author("B", "Oxford")))
				.append("tags", List.of("a", "b")).append("noDefault", 7).append("location", point(40.7128, -74.006));
		store("full", full);

		store("absent", new Document());

		Document nulls = new Document();
		for (String field : List.of("journal", "citationCount", "bytes", "score", "weight", "active", "pubDate", "authors", "tags", "noDefault", "location")) {
			nulls.put(field, null);
		}
		store("nulls", nulls);

		Document partial = new Document().append("journal", "Science").append("citationCount", 12).append("bytes", 20L).append("score", 1.5f)
				.append("weight", 0.5d).append("active", false).append("pubDate", "2020-01-01")
				.append("authors", List.of(author("A", "MIT"), author("B", null), author("C", "Oxford"))).append("tags", Arrays.asList("x", null, ""))
				.append("noDefault", 3).append("location", point(34.0522, -118.2437));
		store("partial", partial);

		Document allMissing = new Document().append("journal", "Cell").append("citationCount", 1).append("bytes", 1L).append("score", 1.0f)
				.append("weight", 2.0d).append("active", true).append("pubDate", "2000-05-05").append("authors", List.of(author("A", null), author("B", null)))
				.append("tags", Arrays.asList(null, null)).append("noDefault", 1).append("location", point(41.8781, -87.6298));
		store("allMissing", allMissing);
	}

	@Test
	@Order(3)
	public void wholeValueDefaultsApplyToAbsentAndNull() throws Exception {
		Assertions.assertEquals(2, count("journal:unknown"));
		Assertions.assertEquals(1, count("journal:nature"));
		Assertions.assertEquals(2, count("citationCount:0"));
		Assertions.assertEquals(4, count("citationCount:[0 TO 10]"));
		Assertions.assertEquals(2, count("bytes:0"));
		Assertions.assertEquals(2, count("score:0.0"));
		Assertions.assertEquals(2, count("weight:1.0"));
		Assertions.assertEquals(3, count("active:false"));
		Assertions.assertEquals(2, count("active:true"));
		Assertions.assertEquals(2, count("pubDate:1970-01-01"));

		Assertions.assertEquals(List.of("absent", "nulls"), ids("journal:unknown"));
		Assertions.assertEquals(List.of("absent", "nulls"), ids("pubDate:1970-01-01"));
		// the geo default sits in Washington DC, far from the three real locations
		Assertions.assertEquals(List.of("absent", "nulls"), ids("location:zl:geo(location 38.9072 -77.0369 10)"));
		Assertions.assertEquals(List.of("full"), ids("location:zl:geo(location 40.7128 -74.006 10)"));
	}

	@Test
	@Order(4)
	public void existsMarkerIsWithheldForDefaultedDocuments() throws Exception {
		for (String field : List.of("journal", "citationCount", "bytes", "score", "weight", "active", "pubDate", "location")) {
			Assertions.assertEquals(3, count(field + ":*"), field);
			Assertions.assertEquals(List.of("absent", "nulls"), ids("-" + field + ":*"), field);
		}
		// control field, unchanged behavior
		Assertions.assertEquals(3, count("noDefault:*"));
		Assertions.assertEquals(List.of("absent", "nulls"), ids("-noDefault:*"));
		Assertions.assertEquals(0, count("noDefault:0"));
	}

	@Test
	@Order(5)
	public void defaultsAreFacetedAndSorted() throws Exception {
		Map<String, Long> journal = facetCounts("journal");
		Assertions.assertEquals(2L, journal.get("unknown"));
		Assertions.assertEquals(1L, journal.get("Nature"));
		Assertions.assertEquals(1L, journal.get("Science"));
		Assertions.assertEquals(1L, journal.get("Cell"));

		Assertions.assertEquals(2L, facetCounts("citationCount").get("0"));
		Assertions.assertEquals(3L, facetCounts("active").get("False"));
		Assertions.assertEquals(2L, facetCounts("active").get("True"));
		Assertions.assertEquals(2L, facetCounts("pubDate").get("1970-01-01"));

		Assertions.assertEquals(List.of("absent", "nulls", "allMissing", "full", "partial"), sortedIds("citationCount", true));
		Assertions.assertEquals(List.of("partial", "full", "allMissing", "absent", "nulls"), sortedIds("citationCount", false));
		Assertions.assertEquals(List.of("absent", "nulls", "allMissing", "partial", "full"), sortedIds("pubDate", true));

		// distance from the default location: the two defaulted documents first at distance zero, then New York, Chicago, Los Angeles
		Search byDistance = new Search(INDEX_NAME).setRealtime(true).setAmount(100).addSort(Sort.geoDistance("location", 38.9072, -77.0369))
				.addSort(new Sort("id").ascending());
		List<String> byDistanceIds = nodeExtension.getClient().search(byDistance).getUniqueIds();
		Assertions.assertEquals(List.of("absent", "nulls", "full", "allMissing", "partial"), byDistanceIds);
	}

	@Test
	@Order(6)
	public void eachElementFillsMissingSubFieldsAndNullElements() throws Exception {
		// whole value missing for absent and nulls, per element for partial and allMissing
		Assertions.assertEquals(List.of("absent", "allMissing", "nulls", "partial"), ids("affiliation:unknown"));
		Assertions.assertEquals(List.of("full", "partial"), ids("affiliation:mit"));
		Assertions.assertEquals(List.of("full", "partial"), ids("affiliation:*"));
		Assertions.assertEquals(List.of("absent", "allMissing", "nulls"), ids("-affiliation:*"));

		// one facet count per document no matter how many elements were filled
		Map<String, Long> affiliation = facetCounts("affiliation");
		Assertions.assertEquals(4L, affiliation.get("unknown"));
		Assertions.assertEquals(2L, affiliation.get("MIT"));
		Assertions.assertEquals(2L, affiliation.get("Oxford"));

		// nulls in a top-level list are filled, empty strings are not
		Assertions.assertEquals(List.of("absent", "allMissing", "nulls", "partial"), ids("tags:untagged"));
		Assertions.assertEquals(List.of("partial"), ids("tags:x"));
		Assertions.assertEquals(List.of("partial"), ids("|||tags|||:3"));
		Assertions.assertEquals(List.of("partial"), ids("|tags|:0"));
		Assertions.assertEquals(List.of("allMissing"), ids("|||tags|||:2 -tags:x -tags:a"));
		Assertions.assertEquals(List.of("absent", "allMissing", "nulls"), ids("-tags:*"));
		Map<String, Long> tags = facetCounts("tags");
		Assertions.assertEquals(4L, tags.get("untagged"));
		Assertions.assertEquals(1L, tags.get("x"));
		Assertions.assertNull(tags.get(""));
	}

	@Test
	@Order(7)
	public void statsIncludeDefaultedDocuments() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		// documented caveat: a zero default counts in stats
		SearchResult searchResult = zuliaWorkPool.search(new Search(INDEX_NAME).setRealtime(true).addStat(new NumericStat("citationCount")));
		ZuliaQuery.FacetStats stat = searchResult.getNumericFieldStat("citationCount");
		Assertions.assertEquals(5, stat.getDocCount());
		Assertions.assertEquals(18, stat.getSum().getLongValue());
	}

	@Test
	@Order(8)
	public void storedDocumentIsNeverModified() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		Document absent = zuliaWorkPool.fetch(new Fetch("absent", INDEX_NAME)).getDocument();
		Assertions.assertFalse(absent.containsKey("citationCount"));
		Assertions.assertFalse(absent.containsKey("journal"));

		Document nulls = zuliaWorkPool.fetch(new Fetch("nulls", INDEX_NAME)).getDocument();
		Assertions.assertTrue(nulls.containsKey("citationCount"));
		Assertions.assertNull(nulls.get("citationCount"));

		Document partial = zuliaWorkPool.fetch(new Fetch("partial", INDEX_NAME)).getDocument();
		List<Document> authors = partial.getList("authors", Document.class);
		Assertions.assertNull(authors.get(1).get("affiliation"));
		Assertions.assertEquals(Arrays.asList("x", null, ""), partial.getList("tags", String.class));
	}

	@Test
	@Order(9)
	public void mismatchedDefaultsAreRejected() {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// hand-built protos, since the builder rejects these first
		assertCreateFails(zuliaWorkPool, rawField("citationCount", FieldConfig.FieldType.NUMERIC_INT, DefaultValue.newBuilder().setLongValue(0).build()),
				"Field <citationCount> is NUMERIC_INT but defaultValue is longValue <0>, expected intValue");
		assertCreateFails(zuliaWorkPool, rawField("citationCount", FieldConfig.FieldType.NUMERIC_INT, DefaultValue.newBuilder().setStringValue("zero").build()),
				"Field <citationCount> is NUMERIC_INT but defaultValue is stringValue <zero>, expected intValue");
		assertCreateFails(zuliaWorkPool, rawField("score", FieldConfig.FieldType.NUMERIC_FLOAT, DefaultValue.newBuilder().setDoubleValue(0).build()),
				"Field <score> is NUMERIC_FLOAT but defaultValue is doubleValue <0.0>, expected floatValue");
		assertCreateFails(zuliaWorkPool, rawField("active", FieldConfig.FieldType.BOOL, DefaultValue.newBuilder().setIntValue(0).build()),
				"Field <active> is BOOL but defaultValue is intValue <0>, expected boolValue");
		assertCreateFails(zuliaWorkPool, rawField("v", FieldConfig.FieldType.VECTOR, DefaultValue.newBuilder().setFloatValue(0).build()),
				"Field <v> is VECTOR which does not support a defaultValue, found floatValue <0.0>");
		assertCreateFails(zuliaWorkPool, rawField("g", FieldConfig.FieldType.GEO_POINT, DefaultValue.newBuilder().setStringValue("x").build()).toBuilder()
						.addIndexAs(IndexAs.newBuilder().setIndexFieldName("g")).build(),
				"Field <g> is GEO_POINT but defaultValue is stringValue <x>, expected geoPointValue");
		assertCreateFails(zuliaWorkPool, rawField("g", FieldConfig.FieldType.GEO_POINT,
				DefaultValue.newBuilder().setGeoPointValue(GeoPointValue.newBuilder().setLatitude(91).setLongitude(0)).build()).toBuilder()
				.addIndexAs(IndexAs.newBuilder().setIndexFieldName("g")).build(), "Field <g> defaultValue latitude <91.0> must be between -90 and 90");
		assertCreateFails(zuliaWorkPool, rawField("citationCount", FieldConfig.FieldType.NUMERIC_INT, DefaultValue.getDefaultInstance()),
				"Field <citationCount> is NUMERIC_INT and its defaultValue must set intValue but no value is set");
	}

	@Test
	@Order(10)
	public void addingADefaultOnUpdateAppliesToNewDocumentsAndToReindex() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("id");
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("status").indexAs(DefaultAnalyzers.LC_KEYWORD).facet());
		indexConfig.setIndexName(UPDATE_INDEX_NAME);
		indexConfig.setNumberOfShards(1);
		zuliaWorkPool.createIndex(indexConfig);

		zuliaWorkPool.store(new Store("s1", UPDATE_INDEX_NAME, ResultDocBuilder.from(new Document("id", "s1"))));
		Assertions.assertEquals(0, count(UPDATE_INDEX_NAME, "status:unknown"));

		zuliaWorkPool.updateIndex(new UpdateIndex(UPDATE_INDEX_NAME).mergeFieldConfig(
				FieldConfigBuilder.createString("status").indexAs(DefaultAnalyzers.LC_KEYWORD).facet().defaultValue("unknown")));

		zuliaWorkPool.store(new Store("s2", UPDATE_INDEX_NAME, ResultDocBuilder.from(new Document("id", "s2"))));
		Assertions.assertEquals(List.of("s2"), ids(UPDATE_INDEX_NAME, "status:unknown"));

		zuliaWorkPool.reindex(UPDATE_INDEX_NAME);
		Assertions.assertEquals(List.of("s1", "s2"), ids(UPDATE_INDEX_NAME, "status:unknown"));
		Assertions.assertEquals(List.of("s1", "s2"), ids(UPDATE_INDEX_NAME, "-status:*"));
	}

	@Test
	@Order(11)
	public void restart() throws Exception {
		nodeExtension.restartNodes();
	}

	@Test
	@Order(12)
	public void confirmAfterRestart() throws Exception {
		wholeValueDefaultsApplyToAbsentAndNull();
		existsMarkerIsWithheldForDefaultedDocuments();
		eachElementFillsMissingSubFieldsAndNullElements();
	}

	private static Document point(double latitude, double longitude) {
		return new Document("latitude", latitude).append("longitude", longitude);
	}

	private static Document author(String name, String affiliation) {
		Document author = new Document("name", name);
		if (affiliation != null) {
			author.put("affiliation", affiliation);
		}
		return author;
	}

	private static void store(String id, Document document) throws Exception {
		document.put("id", id);
		document.put("title", "title of " + id);
		nodeExtension.getClient().store(new Store(id, INDEX_NAME, ResultDocBuilder.from(document)));
	}

	private static long count(String query) throws Exception {
		return count(INDEX_NAME, query);
	}

	private static long count(String indexName, String query) throws Exception {
		return nodeExtension.getClient().search(new Search(indexName).setRealtime(true).addQuery(new FilterQuery(query))).getTotalHits();
	}

	private static List<String> ids(String query) throws Exception {
		return ids(INDEX_NAME, query);
	}

	private static List<String> ids(String indexName, String query) throws Exception {
		Search search = new Search(indexName).setRealtime(true).setAmount(100).addQuery(new FilterQuery(query)).addSort(new Sort("id").ascending());
		return nodeExtension.getClient().search(search).getUniqueIds().stream().sorted().toList();
	}

	private static List<String> sortedIds(String sortField, boolean ascending) throws Exception {
		Sort sort = ascending ? new Sort(sortField).ascending() : new Sort(sortField).descending();
		Search search = new Search(INDEX_NAME).setRealtime(true).setAmount(100).addSort(sort).addSort(new Sort("id").ascending());
		return nodeExtension.getClient().search(search).getUniqueIds();
	}

	private static Map<String, Long> facetCounts(String facet) throws Exception {
		SearchResult searchResult = nodeExtension.getClient().search(new Search(INDEX_NAME).setRealtime(true).addCountFacet(new CountFacet(facet)));
		return searchResult.getFacetCounts(facet).stream().collect(Collectors.toMap(ZuliaQuery.FacetCount::getFacet, ZuliaQuery.FacetCount::getCount));
	}

	private static FieldConfig rawField(String storedFieldName, FieldConfig.FieldType fieldType, DefaultValue defaultValue) {
		return FieldConfig.newBuilder().setStoredFieldName(storedFieldName).setFieldType(fieldType).setDefaultValue(defaultValue).build();
	}

	private static void assertCreateFails(ZuliaWorkPool zuliaWorkPool, FieldConfig fieldConfig, String expectedMessage) {
		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("id");
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		indexConfig.addFieldConfig(fieldConfig);
		indexConfig.setIndexName("fieldDefaultRejected");
		indexConfig.setNumberOfShards(1);

		Exception exception = Assertions.assertThrows(Exception.class, () -> zuliaWorkPool.createIndex(indexConfig));
		Assertions.assertTrue(exception.getMessage().contains(expectedMessage), exception.getMessage());
	}
}
