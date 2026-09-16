package io.zulia.server.test.node;

import io.zulia.DefaultAnalyzers;
import io.zulia.ZuliaFieldConstants;
import io.zulia.client.command.Store;
import io.zulia.client.command.builder.CountFacet;
import io.zulia.client.command.builder.FilterQuery;
import io.zulia.client.command.builder.Search;
import io.zulia.client.command.builder.Sort;
import io.zulia.client.command.builder.VectorTopNQuery;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.SearchResult;
import io.zulia.doc.ResultDocBuilder;
import io.zulia.fields.FieldConfigBuilder;
import io.zulia.message.ZuliaIndex.DefaultValue;
import io.zulia.message.ZuliaIndex.FieldConfig;
import io.zulia.message.ZuliaIndex.FieldConfig.MalformedValueHandling;
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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * FAIL rejects a document with an unparseable value, SKIP drops the element, USE_DEFAULT substitutes the default, and
 * marked documents match {@code _zmff_:storedFieldName}. A field config nothing consumes is never parsed.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class MalformedValueTest {

	@RegisterExtension
	static final NodeExtension nodeExtension = new NodeExtension(1);

	public static final String INDEX_NAME = "malformedValueTest";

	@Test
	@Order(1)
	public void createIndex() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("id");
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createInt("skipCount").index().facet().sort().onMalformed(MalformedValueHandling.SKIP));
		indexConfig.addFieldConfig(
				FieldConfigBuilder.createInt("defaultedCount").index().facet().sort().defaultValue(-1).onMalformed(MalformedValueHandling.USE_DEFAULT));
		indexConfig.addFieldConfig(FieldConfigBuilder.createInt("strictCount").index());
		indexConfig.addFieldConfig(FieldConfigBuilder.createBool("flag").index().facet().sort().onMalformed(MalformedValueHandling.SKIP));
		indexConfig.addFieldConfig(FieldConfigBuilder.createDate("when").index().sort().onMalformed(MalformedValueHandling.SKIP));
		indexConfig.addFieldConfig(FieldConfigBuilder.createInt("facetOnlyCount").facet());
		indexConfig.addFieldConfig(
				FieldConfigBuilder.createInt("authors.papers").indexAs(null, "papers").facetAs("papers").onMalformed(MalformedValueHandling.SKIP));
		indexConfig.addFieldConfig(FieldConfigBuilder.createBool("facetOnlyFlag").facet());
		indexConfig.addFieldConfig(FieldConfigBuilder.createVector("embedding").dimensions(3).index().onMalformed(MalformedValueHandling.SKIP));
		indexConfig.addFieldConfig(FieldConfigBuilder.createVector("strictEmbedding").dimensions(3).index());
		indexConfig.addFieldConfig(FieldConfigBuilder.createGeoPoint("where").index().sort().onMalformed(MalformedValueHandling.SKIP));
		indexConfig.addFieldConfig(FieldConfigBuilder.createGeoPoint("strictWhere").index());
		indexConfig.addFieldConfig(
				FieldConfigBuilder.createGeoPoint("place").index().defaultValue(38.9072, -77.0369).onMalformed(MalformedValueHandling.USE_DEFAULT));
		// a geo point reading top-level keys has no stored field name, so its marker is keyed by the name it is indexed under
		indexConfig.addFieldConfig(FieldConfigBuilder.createGeoPointTopLevel("lat", "lon").indexAsField("position").onMalformed(MalformedValueHandling.SKIP));
		indexConfig.addFieldConfig(FieldConfigBuilder.createInt("typeOnlyCount"));
		indexConfig.setIndexName(INDEX_NAME);
		indexConfig.setNumberOfShards(1);

		zuliaWorkPool.createIndex(indexConfig);
	}

	@Test
	@Order(2)
	public void index() throws Exception {
		store("mixed",
				new Document().append("skipCount", List.of(1, "abc", 3)).append("defaultedCount", List.of(1, "abc", 3)).append("flag", List.of("yes", "maybe"))
						.append("when", List.of("2024-06-17", "not a date"))
						.append("authors", List.of(new Document("papers", 4), new Document("papers", "many"))).append("embedding", List.of(0.1, 0.2))
						.append("where", new Document("type", "Point").append("coordinates", "not a list"))
						.append("place", new Document("latitude", "abc").append("longitude", 1.0)).append("lat", "abc").append("lon", 1.0));
		store("allBad", new Document().append("skipCount", "abc").append("defaultedCount", "abc").append("flag", 2).append("when", new Document("x", 1))
				.append("embedding", "abc").append("where", new Document("latitude", 91.0).append("longitude", 0.0))
				.append("place", List.of(new Document("latitude", 1.0))));
		store("clean", new Document().append("skipCount", 7).append("defaultedCount", "7").append("flag", true).append("when", "2020-01-01")
				.append("authors", List.of(new Document("papers", 9))).append("typeOnlyCount", "n/a").append("embedding", List.of(0.1, 0.2, 0.3))
				.append("where", new Document("latitude", 40.7128).append("longitude", -74.006))
				.append("place", new Document("latitude", 34.0522).append("longitude", -118.2437)));
	}

	@Test
	@Order(3)
	public void skipDropsOnlyTheBadElement() throws Exception {
		Assertions.assertEquals(List.of("mixed"), ids("skipCount:1"));
		Assertions.assertEquals(List.of("mixed"), ids("skipCount:3"));
		Assertions.assertEquals(List.of("mixed"), ids("|||skipCount|||:2"));
		Assertions.assertEquals(List.of("clean"), ids("skipCount:7"));
		Assertions.assertEquals(List.of("clean", "mixed"), ids("flag:true"));
		Assertions.assertEquals(List.of("mixed"), ids("when:2024-06-17"));

		Map<String, Long> skipCount = facetCounts("skipCount");
		Assertions.assertEquals(1L, skipCount.get("1"));
		Assertions.assertEquals(1L, skipCount.get("3"));
		Assertions.assertEquals(1L, skipCount.get("7"));
		Assertions.assertEquals(2L, facetCounts("flag").get("True"));

		// a wrong dimension vector and a string in place of one are skipped, the good vector still answers a kNN query
		Search knn = new Search(INDEX_NAME).setRealtime(true).setAmount(10).addQuery(new VectorTopNQuery(new float[] { 0.1f, 0.2f, 0.3f }, 3, "embedding"));
		Assertions.assertEquals(List.of("clean"), nodeExtension.getClient().search(knn).getUniqueIds());
		Assertions.assertEquals(List.of("clean"), ids("embedding:*"));
		Assertions.assertEquals(List.of("allBad", "mixed"), ids(ZuliaFieldConstants.MALFORMED_FIELDS_LIST_FIELD + ":embedding"));

		// a GeoJSON point without numeric coordinates and an out of range latitude are skipped
		Assertions.assertEquals(List.of("clean"), ids("where:zl:geo(where 40.7128 -74.006 10)"));
		Assertions.assertEquals(List.of("clean"), ids("where:*"));
		Assertions.assertEquals(List.of("allBad", "mixed"), ids(ZuliaFieldConstants.MALFORMED_FIELDS_LIST_FIELD + ":where"));
	}

	@Test
	@Order(4)
	public void useDefaultReplacesTheBadElement() throws Exception {
		Assertions.assertEquals(List.of("allBad", "mixed"), ids("defaultedCount:[-1 TO -1]"));
		Assertions.assertEquals(List.of("mixed"), ids("|||defaultedCount|||:3"));
		Assertions.assertEquals(List.of("clean"), ids("defaultedCount:7"));
		Assertions.assertEquals(2L, facetCounts("defaultedCount").get("-1"));
		// the replaced value participates in sorting like any other
		Assertions.assertEquals(List.of("allBad", "mixed", "clean"), sortedIds("defaultedCount"));

		// a geo point with a non-numeric coordinate, or one coordinate without the other, is indexed at the default location
		Assertions.assertEquals(List.of("allBad", "mixed"), ids("place:zl:geo(place 38.9072 -77.0369 10)"));
		Assertions.assertEquals(List.of("clean"), ids("place:zl:geo(place 34.0522 -118.2437 10)"));
		Assertions.assertEquals(List.of("allBad", "mixed"), ids("-place:*"));
		Assertions.assertEquals(List.of("allBad", "mixed"), ids(ZuliaFieldConstants.MALFORMED_FIELDS_LIST_FIELD + ":place"));
	}

	@Test
	@Order(5)
	public void malformedMarkerFindsTheDocumentsToRepair() throws Exception {
		Assertions.assertEquals(List.of("allBad", "mixed"), ids(ZuliaFieldConstants.MALFORMED_FIELDS_LIST_FIELD + ":skipCount"));
		Assertions.assertEquals(List.of("allBad", "mixed"), ids(ZuliaFieldConstants.MALFORMED_FIELDS_LIST_FIELD + ":defaultedCount"));
		Assertions.assertEquals(List.of("allBad", "mixed"), ids(ZuliaFieldConstants.MALFORMED_FIELDS_LIST_FIELD + ":flag"));
		Assertions.assertEquals(List.of("allBad", "mixed"), ids(ZuliaFieldConstants.MALFORMED_FIELDS_LIST_FIELD + ":when"));
		Assertions.assertEquals(List.of("clean"), ids("*:* -" + ZuliaFieldConstants.MALFORMED_FIELDS_LIST_FIELD + ":skipCount"));
		// marker keyed by stored name, not the indexed name
		Assertions.assertEquals(List.of("mixed"), ids(ZuliaFieldConstants.MALFORMED_FIELDS_LIST_FIELD + ":authors.papers"));
		Assertions.assertEquals(List.of(), ids(ZuliaFieldConstants.MALFORMED_FIELDS_LIST_FIELD + ":papers"));
		Assertions.assertEquals(List.of("mixed"), ids("papers:4"));
		Assertions.assertEquals(1L, facetCounts("papers").get("4"));
		// a geo point with no stored field name is still findable, keyed by the name it is indexed under
		Assertions.assertEquals(List.of("mixed"), ids(ZuliaFieldConstants.MALFORMED_FIELDS_LIST_FIELD + ":position"));
		// any malformed value at all, through the ordinary wildcard rewrite
		Assertions.assertEquals(List.of("allBad", "mixed"), ids(ZuliaFieldConstants.MALFORMED_FIELDS_LIST_FIELD + ":*"));
		Assertions.assertEquals(List.of("clean"), ids("-" + ZuliaFieldConstants.MALFORMED_FIELDS_LIST_FIELD + ":*"));
	}

	@Test
	@Order(6)
	public void existsMarkerNeedsOneRealValue() throws Exception {
		// field:* needs one real value
		Assertions.assertEquals(List.of("clean", "mixed"), ids("skipCount:*"));
		Assertions.assertEquals(List.of("allBad"), ids("-skipCount:*"));
		Assertions.assertEquals(List.of("clean", "mixed"), ids("defaultedCount:*"));
		Assertions.assertEquals(List.of("allBad"), ids("-defaultedCount:*"));
		Assertions.assertEquals(List.of("clean", "mixed"), ids("flag:*"));
		Assertions.assertEquals(List.of("clean", "mixed"), ids("when:*"));
	}

	@Test
	@Order(7)
	public void failRejectsTheDocumentAndNamesTheValue() throws Exception {
		Exception badString = Assertions.assertThrows(Exception.class, () -> store("strict1", new Document("strictCount", "abc")));
		Assertions.assertTrue(badString.getMessage().contains("String value <abc> for field <strictCount> cannot be parsed as NUMERIC_INT"),
				badString.getMessage());

		Exception badElement = Assertions.assertThrows(Exception.class, () -> store("strict2", new Document("strictCount", List.of(1, "abc"))));
		Assertions.assertTrue(badElement.getMessage().contains("cannot be parsed as NUMERIC_INT"), badElement.getMessage());

		Exception badType = Assertions.assertThrows(Exception.class, () -> store("strict3", new Document("strictCount", new Document("x", 1))));
		Assertions.assertTrue(badType.getMessage().contains("Expecting Number or numeric String for field <strictCount> and found <Document>"),
				badType.getMessage());

		// facet-only fields are as strict as indexed ones
		Exception facetOnlyCount = Assertions.assertThrows(Exception.class, () -> store("strict4", new Document("facetOnlyCount", "abc")));
		Assertions.assertTrue(facetOnlyCount.getMessage().contains("cannot be parsed as NUMERIC_INT"), facetOnlyCount.getMessage());
		Exception facetOnlyFlag = Assertions.assertThrows(Exception.class, () -> store("strict5", new Document("facetOnlyFlag", "maybe")));
		Assertions.assertTrue(facetOnlyFlag.getMessage().contains("for field <facetOnlyFlag> and found <maybe>"), facetOnlyFlag.getMessage());

		Exception badVectorElement = Assertions.assertThrows(Exception.class, () -> store("strict6", new Document("strictEmbedding", List.of(0.1, "x", 0.3))));
		Assertions.assertTrue(badVectorElement.getMessage().contains("Vector field <strictEmbedding> element 1 is <String> instead of a number"),
				badVectorElement.getMessage());
		Exception badVectorType = Assertions.assertThrows(Exception.class, () -> store("strict7", new Document("strictEmbedding", "abc")));
		Assertions.assertTrue(badVectorType.getMessage().contains("Expecting a list of numbers for vector field <strictEmbedding> and found <String>"),
				badVectorType.getMessage());
		Exception badDimension = Assertions.assertThrows(Exception.class, () -> store("strict8", new Document("strictEmbedding", List.of(0.1, 0.2))));
		Assertions.assertTrue(badDimension.getMessage().contains("Vector field <strictEmbedding> is configured for 3 dimensions but the document has 2"),
				badDimension.getMessage());

		Exception badCoordinate = Assertions.assertThrows(Exception.class,
				() -> store("strict9", new Document("strictWhere", new Document("latitude", "abc").append("longitude", 1.0))));
		Assertions.assertTrue(badCoordinate.getMessage()
						.contains("<latitude> and <longitude> must both be numbers and found <abc> and <1.0> for geo point field <strictWhere>"),
				badCoordinate.getMessage());
		Exception badGeoJson = Assertions.assertThrows(Exception.class,
				() -> store("strict10", new Document("strictWhere", new Document("type", "Point").append("coordinates", "not a list"))));
		Assertions.assertTrue(badGeoJson.getMessage().contains("GeoJSON Point coordinates must be [longitude, latitude] numbers and found <not a list>"),
				badGeoJson.getMessage());
		Exception outOfRange = Assertions.assertThrows(Exception.class,
				() -> store("strict11", new Document("strictWhere", new Document("latitude", 91.0).append("longitude", 0.0))));
		Assertions.assertTrue(outOfRange.getMessage().contains("latitude <91.0> must be between -90 and 90 for geo point field <strictWhere>"),
				outOfRange.getMessage());
		Exception badGeoType = Assertions.assertThrows(Exception.class, () -> store("strict12", new Document("strictWhere", "abc")));
		Assertions.assertTrue(
				badGeoType.getMessage().contains("Expecting a Document with latitude and longitude or a GeoJSON Point for geo point field <strictWhere>"),
				badGeoType.getMessage());
		// a geo point document carrying neither coordinate key is absent, not malformed
		store("noCoordinates", new Document("strictWhere", new Document("name", "somewhere")));

		Assertions.assertEquals(4, nodeExtension.getClient().search(new Search(INDEX_NAME).setRealtime(true)).getTotalHits());
	}

	@Test
	@Order(8)
	public void unsupportedHandlingIsRejected() {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// SKIP needs no default, so a vector field takes it. USE_DEFAULT needs a default and vectors have none
		FieldConfig vectorSkip = FieldConfig.newBuilder().setStoredFieldName("v").setFieldType(FieldConfig.FieldType.VECTOR)
				.setMalformedValueHandling(MalformedValueHandling.SKIP).build();
		Assertions.assertDoesNotThrow(() -> createRejectedIndex(zuliaWorkPool, vectorSkip));
		assertCreateFails(zuliaWorkPool, FieldConfig.newBuilder().setStoredFieldName("v").setFieldType(FieldConfig.FieldType.VECTOR)
						.setMalformedValueHandling(MalformedValueHandling.USE_DEFAULT).build(),
				"Field <v> is VECTOR which takes no defaultValue, so malformedValueHandling USE_DEFAULT is not available");
		assertCreateFails(zuliaWorkPool, FieldConfig.newBuilder().setStoredFieldName("g").setFieldType(FieldConfig.FieldType.GEO_POINT)
						.addIndexAs(IndexAs.newBuilder().setIndexFieldName("g")).setMalformedValueHandling(MalformedValueHandling.USE_DEFAULT).build(),
				"Field <g> has malformedValueHandling USE_DEFAULT but no defaultValue is set");
		assertCreateFails(zuliaWorkPool, FieldConfig.newBuilder().setStoredFieldName("n").setFieldType(FieldConfig.FieldType.NUMERIC_INT)
						.setMalformedValueHandling(MalformedValueHandling.USE_DEFAULT).build(),
				"Field <n> has malformedValueHandling USE_DEFAULT but no defaultValue is set");
		assertCreateFails(zuliaWorkPool,
				FieldConfig.newBuilder().setStoredFieldName("n").setFieldType(FieldConfig.FieldType.NUMERIC_INT).setMalformedValueHandlingValue(9).build(),
				"Field <n> has an unrecognized malformedValueHandling <9>");
		// a default is accepted with USE_DEFAULT and with FAIL
		FieldConfig accepted = FieldConfig.newBuilder().setStoredFieldName("n").setFieldType(FieldConfig.FieldType.NUMERIC_INT)
				.setDefaultValue(DefaultValue.newBuilder().setIntValue(0)).setMalformedValueHandling(MalformedValueHandling.USE_DEFAULT).build();
		Assertions.assertDoesNotThrow(() -> createRejectedIndex(zuliaWorkPool, accepted));
	}

	@Test
	@Order(9)
	public void typeOnlyFieldIsNeverParsed() throws Exception {
		// unconsumed field, never parsed
		Assertions.assertEquals(List.of("clean"), ids("id:clean"));
		Assertions.assertEquals(List.of(), ids(ZuliaFieldConstants.MALFORMED_FIELDS_LIST_FIELD + ":typeOnlyCount"));
	}

	@Test
	@Order(10)
	public void markerFieldsAreNotListedFields() throws Exception {
		List<String> fieldNames = nodeExtension.getClient().getFields(INDEX_NAME).getFieldNames();
		Assertions.assertTrue(fieldNames.contains("skipCount"));
		Assertions.assertFalse(fieldNames.contains(ZuliaFieldConstants.FIELDS_LIST_FIELD));
		Assertions.assertFalse(fieldNames.contains(ZuliaFieldConstants.MALFORMED_FIELDS_LIST_FIELD));
	}

	private static void store(String id, Document document) throws Exception {
		document.put("id", id);
		nodeExtension.getClient().store(new Store(id, INDEX_NAME, ResultDocBuilder.from(document)));
	}

	private static List<String> ids(String query) throws Exception {
		Search search = new Search(INDEX_NAME).setRealtime(true).setAmount(100).addQuery(new FilterQuery(query)).addSort(new Sort("id").ascending());
		return nodeExtension.getClient().search(search).getUniqueIds();
	}

	private static List<String> sortedIds(String sortField) throws Exception {
		Search search = new Search(INDEX_NAME).setRealtime(true).setAmount(100).addSort(new Sort(sortField).ascending()).addSort(new Sort("id").ascending());
		return nodeExtension.getClient().search(search).getUniqueIds();
	}

	private static Map<String, Long> facetCounts(String facet) throws Exception {
		SearchResult searchResult = nodeExtension.getClient().search(new Search(INDEX_NAME).setRealtime(true).addCountFacet(new CountFacet(facet)));
		return searchResult.getFacetCounts(facet).stream().collect(Collectors.toMap(ZuliaQuery.FacetCount::getFacet, ZuliaQuery.FacetCount::getCount));
	}

	private static void createRejectedIndex(ZuliaWorkPool zuliaWorkPool, FieldConfig fieldConfig) throws Exception {
		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("id");
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		indexConfig.addFieldConfig(fieldConfig);
		indexConfig.setIndexName("malformedHandlingRejected");
		indexConfig.setNumberOfShards(1);
		zuliaWorkPool.createIndex(indexConfig);
		zuliaWorkPool.deleteIndex("malformedHandlingRejected");
	}

	private static void assertCreateFails(ZuliaWorkPool zuliaWorkPool, FieldConfig fieldConfig, String expectedMessage) {
		Exception exception = Assertions.assertThrows(Exception.class, () -> createRejectedIndex(zuliaWorkPool, fieldConfig));
		Assertions.assertTrue(exception.getMessage().contains(expectedMessage), exception.getMessage());
	}
}
