package io.zulia.server.test.util;

import io.zulia.message.ZuliaIndex.FieldConfig;
import io.zulia.message.ZuliaIndex.FieldConfig.FieldType;
import io.zulia.message.ZuliaIndex.IndexAs;
import io.zulia.server.index.field.StoredFieldHandler;
import io.zulia.server.index.field.StoredFieldValueResolver;
import io.zulia.util.LatLon;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A value a field type reads as absent, such as a blank date string, is not indexed but still holds its place in the
 * resolved list, because the list length the indexer writes counts what the document held.
 */
public class StoredFieldValueResolverTest {

	private static FieldConfig field(String storedFieldName, FieldType fieldType) {
		return FieldConfig.newBuilder().setStoredFieldName(storedFieldName).setFieldType(fieldType)
				.addIndexAs(IndexAs.newBuilder().setIndexFieldName(storedFieldName)).build();
	}

	private static List<Object> resolved(Document document, FieldConfig fieldConfig) {
		StoredFieldHandler handler = StoredFieldValueResolver.resolve(document, fieldConfig, null);
		Assertions.assertTrue(handler.existsMarker(), "a field with no default and no lenient handling is present when the path resolves");
		Assertions.assertFalse(handler.hasMalformedValues());
		return allValues(handler);
	}

	private static List<Object> allValues(StoredFieldHandler handler) {
		List<Object> values = new ArrayList<>();
		handler.onAllValues(values::add);
		return values;
	}

	private static List<Object> uniqueValues(StoredFieldHandler handler) {
		List<Object> values = new ArrayList<>();
		handler.onUniqueValues(values::add);
		return values;
	}

	@Test
	public void blankDateInAListKeepsTheListLength() {
		FieldConfig pubDate = field("pubDate", FieldType.DATE);

		List<?> values = resolved(new Document("pubDate", Arrays.asList("", "2024-01-01")), pubDate);
		Assertions.assertEquals(2, values.size(), "the blank is not a date but the document held two elements");
		Assertions.assertEquals("", values.get(0));
		Assertions.assertInstanceOf(Date.class, values.get(1));

		Assertions.assertEquals(1, resolved(new Document("pubDate", ""), pubDate).size());
		Assertions.assertEquals(List.of(), resolved(new Document("pubDate", List.of()), pubDate));
	}

	@Test
	public void aBlankStringIsAnOrdinaryValueForAStringField() {
		List<?> values = resolved(new Document("title", Arrays.asList("", "Dune")), field("title", FieldType.STRING));
		Assertions.assertEquals(List.of("", "Dune"), values);
	}

	@Test
	public void aGeoPointCarryingNoCoordinatesKeepsItsPlace() {
		FieldConfig location = FieldConfig.newBuilder().setStoredFieldName("location").setFieldType(FieldType.GEO_POINT)
				.addIndexAs(IndexAs.newBuilder().setIndexFieldName("location")).build();

		// a document with neither coordinate key is absent rather than malformed, and the indexer skips it
		List<?> values = resolved(new Document("location", new Document("name", "somewhere")), location);
		Assertions.assertEquals(1, values.size());
		Assertions.assertInstanceOf(Document.class, values.getFirst());
	}

	/**
	 * A single stored value skips the element and value lists, so this pins that the short path and the loop agree on
	 * both the resolved value and the two markers for every field type.
	 */
	@Test
	public void aSingleValueResolvesTheSameAsAOneElementList() {
		Map<FieldType, Object> valueByType = new LinkedHashMap<>();
		valueByType.put(FieldType.STRING, "Dune");
		valueByType.put(FieldType.NUMERIC_INT, "7");
		valueByType.put(FieldType.NUMERIC_LONG, 9000000000L);
		valueByType.put(FieldType.NUMERIC_FLOAT, 1.5f);
		valueByType.put(FieldType.NUMERIC_DOUBLE, 2.5d);
		valueByType.put(FieldType.BOOL, "yes");
		valueByType.put(FieldType.DATE, "2024-06-17");
		valueByType.put(FieldType.GEO_POINT, new Document("latitude", 40.7128).append("longitude", -74.006));

		valueByType.forEach((fieldType, value) -> {
			FieldConfig fieldConfig = field("f", fieldType);
			StoredFieldHandler single = StoredFieldValueResolver.resolve(new Document("f", value), fieldConfig, null);
			StoredFieldHandler inList = StoredFieldValueResolver.resolve(new Document("f", List.of(value)), fieldConfig, null);

			Assertions.assertEquals(inList.existsMarker(), single.existsMarker(), fieldType + " exists marker");
			Assertions.assertEquals(inList.hasMalformedValues(), single.hasMalformedValues(), fieldType + " malformed marker");
			Assertions.assertEquals(allValues(inList), allValues(single), fieldType + " values");
		});

		// a value the type reads as absent takes the same short path and keeps its place
		FieldConfig pubDate = field("pubDate", FieldType.DATE);
		Assertions.assertEquals(List.of(""), allValues(StoredFieldValueResolver.resolve(new Document("pubDate", ""), pubDate, null)));
	}

	@Test
	public void aGeoPointReadingTopLevelKeysAcceptsBothStoredForms() {
		// an empty stored field name means the document itself is the point
		FieldConfig topLevel = FieldConfig.newBuilder().setStoredFieldName("").setFieldType(FieldType.GEO_POINT)
				.addIndexAs(IndexAs.newBuilder().setIndexFieldName("position")).build();

		Document withKeys = new Document("id", "1").append("latitude", 40.7128).append("longitude", -74.006);
		Assertions.assertEquals(List.of(new LatLon(40.7128, -74.006)), resolved(withKeys, topLevel));

		Document geoJson = new Document("id", "2").append("type", "Point").append("coordinates", List.of(-74.006, 40.7128));
		Assertions.assertEquals(List.of(new LatLon(40.7128, -74.006)), resolved(geoJson, topLevel));

		// a document carrying neither form has no point at all
		StoredFieldHandler neither = StoredFieldValueResolver.resolve(new Document("id", "3"), topLevel, null);
		Assertions.assertFalse(neither.isPresent());
	}

	@Test
	public void typedValuesAreProducedForEveryOrdinaryType() {
		Assertions.assertEquals(List.of(7), resolved(new Document("count", "7"), field("count", FieldType.NUMERIC_INT)));
		Assertions.assertEquals(List.of(true), resolved(new Document("flag", "yes"), field("flag", FieldType.BOOL)));
		Assertions.assertInstanceOf(Date.class, resolved(new Document("when", "2024-06-17"), field("when", FieldType.DATE)).getFirst());
	}

	@Test
	public void everyOccurrenceIsIndexedButOnlyDistinctOnesAreFacetedAndSorted() {
		FieldConfig keywords = field("keywords", FieldType.STRING);
		StoredFieldHandler handler = StoredFieldValueResolver.resolve(new Document("keywords", List.of("a", "b", "a")), keywords, null);

		Assertions.assertEquals(List.of("a", "b", "a"), allValues(handler));
		Assertions.assertEquals(List.of("a", "b"), uniqueValues(handler));
		// the deduplicated view is built once and reused, so asking twice gives the same answer
		Assertions.assertEquals(List.of("a", "b"), uniqueValues(handler));
	}

	@Test
	public void aSingleValueNeedsNoDeduplication() {
		StoredFieldHandler handler = StoredFieldValueResolver.resolve(new Document("title", "only"), field("title", FieldType.STRING), null);

		Assertions.assertEquals(List.of("only"), allValues(handler));
		Assertions.assertEquals(List.of("only"), uniqueValues(handler));
		Assertions.assertFalse(handler.hasDefaultedValues());
		Assertions.assertFalse(handler.hasMalformedValues());
	}

	@Test
	public void theValueCountIsTheListLengthTheIndexerRecords() {
		FieldConfig keywords = field("keywords", FieldType.STRING);

		Assertions.assertEquals(0, StoredFieldValueResolver.resolve(new Document("keywords", List.of()), keywords, null).valueCount());
		Assertions.assertEquals(1, StoredFieldValueResolver.resolve(new Document("keywords", "one"), keywords, null).valueCount());
		Assertions.assertEquals(3, StoredFieldValueResolver.resolve(new Document("keywords", List.of("a", "b", "a")), keywords, null).valueCount());
		// a nested list is flattened before it is counted
		Assertions.assertEquals(3,
				StoredFieldValueResolver.resolve(new Document("keywords", List.of(List.of("a", "b"), List.of("c"))), keywords, null).valueCount());
		// a value the type reads as absent still holds its place
		FieldConfig pubDate = field("pubDate", FieldType.DATE);
		Assertions.assertEquals(2, StoredFieldValueResolver.resolve(new Document("pubDate", Arrays.asList("", "2024-01-01")), pubDate, null).valueCount());
		// a vector is one value whose numbers are not elements
		FieldConfig vector = FieldConfig.newBuilder().setStoredFieldName("v").setFieldType(FieldType.VECTOR)
				.addIndexAs(IndexAs.newBuilder().setIndexFieldName("v")).build();
		Assertions.assertEquals(1, StoredFieldValueResolver.resolve(new Document("v", List.of(0.1d, 0.2d, 0.3d)), vector, null).valueCount());
	}

	@Test
	public void aFieldWhoseValuesWereAllDroppedIsStillPresentSoTheMarkerIsWritten() {
		FieldConfig skipping = FieldConfig.newBuilder().setStoredFieldName("n").setFieldType(FieldType.NUMERIC_INT)
				.addIndexAs(IndexAs.newBuilder().setIndexFieldName("n")).setMalformedValueHandling(FieldConfig.MalformedValueHandling.SKIP).build();

		StoredFieldHandler handler = StoredFieldValueResolver.resolve(new Document("n", List.of("not a number")), skipping, null);

		Assertions.assertTrue(handler.isPresent(), "the malformed marker still has to be written");
		Assertions.assertEquals(0, handler.valueCount());
		Assertions.assertTrue(handler.hasMalformedValues());
		Assertions.assertTrue(allValues(handler).isEmpty());
	}

	@Test
	public void useDefaultWithoutAValidatedDefaultSkipsTheValueInsteadOfFailing() {
		FieldConfig useDefault = FieldConfig.newBuilder().setStoredFieldName("n").setFieldType(FieldType.NUMERIC_INT)
				.addIndexAs(IndexAs.newBuilder().setIndexFieldName("n")).setMalformedValueHandling(FieldConfig.MalformedValueHandling.USE_DEFAULT).build();

		// a null FieldDefault is what the loader hands over when the configured default never passed validation
		StoredFieldHandler handler = StoredFieldValueResolver.resolve(new Document("n", List.of("7", "bad")), useDefault, null);

		Assertions.assertEquals(List.of(7), allValues(handler), "the malformed value is skipped and the rest survive");
		Assertions.assertTrue(handler.hasMalformedValues());
		Assertions.assertFalse(handler.hasDefaultedValues());
	}
}
