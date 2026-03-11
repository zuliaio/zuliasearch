package io.zulia.server.test.node;

import io.zulia.DefaultAnalyzers;
import io.zulia.client.command.Store;
import io.zulia.client.command.builder.FilterQuery;
import io.zulia.client.command.builder.ScoredQuery;
import io.zulia.client.command.builder.Search;
import io.zulia.client.command.builder.Sort;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.CompleteResult;
import io.zulia.client.result.SearchResult;
import io.zulia.doc.ResultDocBuilder;
import io.zulia.fields.FieldConfigBuilder;
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

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class GeoPointTest {

	@RegisterExtension
	static final NodeExtension nodeExtension = new NodeExtension(1);

	public static final String GEO_TEST_INDEX = "geoTest";

	private static final double NYC_LAT = 40.7128;
	private static final double NYC_LON = -74.0060;
	private static final double LA_LAT = 34.0522;
	private static final double LA_LON = -118.2437;
	private static final double CHICAGO_LAT = 41.8781;
	private static final double CHICAGO_LON = -87.6298;
	private static final double LONDON_LAT = 51.5074;
	private static final double LONDON_LON = -0.1278;

	@Test
	@Order(1)
	public void createIndex() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		ClientIndexConfig indexConfig = new ClientIndexConfig();

		indexConfig.addDefaultSearchField("name");
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD));
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("name").indexAs(DefaultAnalyzers.STANDARD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createInt("year").index().sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createGeoPoint("location").index().sort());

		indexConfig.setIndexName(GEO_TEST_INDEX);
		indexConfig.setNumberOfShards(2);
		indexConfig.setShardCommitInterval(20);

		zuliaWorkPool.createIndex(indexConfig);
	}

	@Test
	@Order(2)
	public void storeDocuments() throws Exception {
		indexRecord(1, "New York", 2020, NYC_LAT, NYC_LON);
		indexRecord(2, "Los Angeles", 2021, LA_LAT, LA_LON);
		indexRecord(3, "Chicago", 2022, CHICAGO_LAT, CHICAGO_LON);
		indexRecord(4, "London", 2020, LONDON_LAT, LONDON_LON);
	}

	private void indexRecord(int id, String name, int year, double lat, double lon) throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		String uniqueId = String.valueOf(id);

		Document mongoDocument = new Document();
		mongoDocument.put("id", uniqueId);
		mongoDocument.put("name", name);
		mongoDocument.put("year", year);

		Document location = new Document();
		location.put("latitude", lat);
		location.put("longitude", lon);
		mongoDocument.put("location", location);

		Store s = new Store(uniqueId, GEO_TEST_INDEX);
		s.setResultDocument(ResultDocBuilder.newBuilder().setDocument(mongoDocument));
		zuliaWorkPool.store(s);
	}

	@Test
	@Order(3)
	public void geoDistanceQuery() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		Search search = new Search(GEO_TEST_INDEX).setAmount(10).setRealtime(true);
		search.addQuery(new FilterQuery("location:zl:geo(location " + NYC_LAT + " " + NYC_LON + " 200)"));
		SearchResult result = zuliaWorkPool.search(search);
		Assertions.assertEquals(1, result.getTotalHits());
		Assertions.assertEquals("New York", result.getFirstDocument().getString("name"));

		search = new Search(GEO_TEST_INDEX).setAmount(10).setRealtime(true);
		search.addQuery(new FilterQuery("location:zl:geo(location " + NYC_LAT + " " + NYC_LON + " 1200)"));
		result = zuliaWorkPool.search(search);
		Assertions.assertEquals(2, result.getTotalHits());

		// 4000 km from NYC covers NYC, Chicago, LA but not London (~5570 km)
		search = new Search(GEO_TEST_INDEX).setAmount(10).setRealtime(true);
		search.addQuery(new FilterQuery("location:zl:geo(location " + NYC_LAT + " " + NYC_LON + " 4000)"));
		result = zuliaWorkPool.search(search);
		Assertions.assertEquals(3, result.getTotalHits());

		search = new Search(GEO_TEST_INDEX).setAmount(10).setRealtime(true);
		search.addQuery(new FilterQuery("location:zl:geo(location " + NYC_LAT + " " + NYC_LON + " 10000)"));
		result = zuliaWorkPool.search(search);
		Assertions.assertEquals(4, result.getTotalHits());
	}

	@Test
	@Order(4)
	public void geoBboxQuery() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		Search search = new Search(GEO_TEST_INDEX).setAmount(10).setRealtime(true);
		search.addQuery(new FilterQuery("location:zl:geoBbox(location 25 50 -130 -60)"));
		SearchResult result = zuliaWorkPool.search(search);
		Assertions.assertEquals(3, result.getTotalHits());

		search = new Search(GEO_TEST_INDEX).setAmount(10).setRealtime(true);
		search.addQuery(new FilterQuery("location:zl:geoBbox(location 40.5 41.0 -74.5 -73.5)"));
		result = zuliaWorkPool.search(search);
		Assertions.assertEquals(1, result.getTotalHits());
		Assertions.assertEquals("New York", result.getFirstDocument().getString("name"));

		search = new Search(GEO_TEST_INDEX).setAmount(10).setRealtime(true);
		search.addQuery(new FilterQuery("location:zl:geoBbox(location 51.0 52.0 -1.0 1.0)"));
		result = zuliaWorkPool.search(search);
		Assertions.assertEquals(1, result.getTotalHits());
		Assertions.assertEquals("London", result.getFirstDocument().getString("name"));
	}

	@Test
	@Order(5)
	public void geoDistanceSort() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		Search search = new Search(GEO_TEST_INDEX).setAmount(10).setRealtime(true);
		search.addSort(Sort.geoDistance("location", NYC_LAT, NYC_LON));
		SearchResult result = zuliaWorkPool.search(search);

		List<CompleteResult> results = result.getCompleteResults();
		Assertions.assertEquals(4, results.size());

		Assertions.assertEquals("New York", results.getFirst().getDocument().getString("name"));

		ZuliaQuery.SortValues sortValues = results.getFirst().getSortValues();
		Assertions.assertTrue(sortValues.getSortValueCount() > 0);
		double nycDistAsc = sortValues.getSortValue(0).getDoubleValue();
		Assertions.assertTrue(nycDistAsc < 1.0, "NYC distance from NYC should be < 1 km, was " + nycDistAsc);

		double prevDist = -1;
		String[] expectedAscOrder = { "New York", "Chicago", "Los Angeles", "London" };
		for (int i = 0; i < results.size(); i++) {
			CompleteResult cr = results.get(i);
			double dist = cr.getSortValues().getSortValue(0).getDoubleValue();
			Assertions.assertTrue(dist > prevDist, "Sort order should be ascending by distance");
			Assertions.assertEquals(expectedAscOrder[i], cr.getDocument().getString("name"),
					"Ascending order position " + i + " should be " + expectedAscOrder[i]);
			prevDist = dist;
		}

		double londonDistAsc = results.getLast().getSortValues().getSortValue(0).getDoubleValue();
		Assertions.assertTrue(londonDistAsc > 5000 && londonDistAsc < 6000,
				"London distance from NYC should be ~5570 km, was " + londonDistAsc);

		search = new Search(GEO_TEST_INDEX).setAmount(10).setRealtime(true);
		search.addSort(Sort.geoDistance("location", NYC_LAT, NYC_LON).descending());
		result = zuliaWorkPool.search(search);
		results = result.getCompleteResults();

		String[] expectedDescOrder = { "London", "Los Angeles", "Chicago", "New York" };
		for (int i = 0; i < results.size(); i++) {
			Assertions.assertEquals(expectedDescOrder[i], results.get(i).getDocument().getString("name"),
					"Descending order position " + i + " should be " + expectedDescOrder[i]);
		}

		double londonDistDesc = results.getFirst().getSortValues().getSortValue(0).getDoubleValue();
		Assertions.assertTrue(londonDistDesc > 5000 && londonDistDesc < 6000,
				"Descending London distance should be ~5570 km (same as ascending), was " + londonDistDesc);

		double nycDistDesc = results.getLast().getSortValues().getSortValue(0).getDoubleValue();
		Assertions.assertTrue(nycDistDesc < 1.0, "Descending NYC distance from NYC should be < 1 km, was " + nycDistDesc);

		Assertions.assertEquals(londonDistAsc, londonDistDesc, 1.0,
				"Ascending and descending should report same distance for London");
	}

	@Test
	@Order(6)
	public void geoDistanceScoreFunction() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		Search search = new Search(GEO_TEST_INDEX).setAmount(10).setRealtime(true);
		ScoredQuery sq = new ScoredQuery("*");
		sq.setScoreFunction("1.0 / (1.0 + geodist(location, " + NYC_LAT + ", " + NYC_LON + "))");
		search.addQuery(sq);

		SearchResult result = zuliaWorkPool.search(search);
		Assertions.assertEquals(4, result.getTotalHits());

		List<CompleteResult> results = result.getCompleteResults();
		float firstScore = results.getFirst().getScore();
		float lastScore = results.getLast().getScore();
		Assertions.assertTrue(firstScore > lastScore, "Closer city should have higher score");

		Assertions.assertTrue(firstScore > 0.9, "NYC score should be ~1.0 (distance ~0 km), was " + firstScore);

		// 1/(1+5570) ≈ 0.00018 — verifies geodist() returns km not meters
		Assertions.assertTrue(lastScore > 0.0001,
				"London score should be ~0.00018 (geodist in km), was " + lastScore);
		Assertions.assertTrue(lastScore < 0.001,
				"London score should be ~0.00018 (geodist in km), was " + lastScore);
	}

	@Test
	@Order(7)
	public void negativeNumberInRegularQuery() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		Search search = new Search(GEO_TEST_INDEX).setAmount(10).setRealtime(true);
		search.addQuery(new FilterQuery("-year:2020"));
		SearchResult result = zuliaWorkPool.search(search);
		Assertions.assertEquals(2, result.getTotalHits());

		search = new Search(GEO_TEST_INDEX).setAmount(10).setRealtime(true);
		search.addQuery(new FilterQuery("location:zl:geo(location " + NYC_LAT + " " + NYC_LON + " 10000)"));
		search.addQuery(new FilterQuery("-year:2020"));
		result = zuliaWorkPool.search(search);
		Assertions.assertEquals(2, result.getTotalHits());

		search = new Search(GEO_TEST_INDEX).setAmount(10).setRealtime(true);
		search.addQuery(new FilterQuery("year:(-2020 OR -2021)"));
		result = zuliaWorkPool.search(search);
		Assertions.assertEquals(1, result.getTotalHits());
	}

	@Test
	@Order(8)
	public void geoWithNegativeCoordinates() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		Search search = new Search(GEO_TEST_INDEX).setAmount(10).setRealtime(true);
		search.addQuery(new FilterQuery("location:zl:geo(location " + LA_LAT + " " + LA_LON + " 100)"));
		SearchResult result = zuliaWorkPool.search(search);
		Assertions.assertEquals(1, result.getTotalHits());
		Assertions.assertEquals("Los Angeles", result.getFirstDocument().getString("name"));
	}

	public static final String GEO_CUSTOM_INDEX = "geoCustomTest";
	public static final String GEO_GEOJSON_INDEX = "geoGeoJsonTest";
	public static final String GEO_TOPLEVEL_INDEX = "geoTopLevelTest";

	@Test
	@Order(9)
	public void customLatLonKeys() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD));
		indexConfig.addFieldConfig(FieldConfigBuilder.createGeoPoint("coords", "lat", "lng").index().sort());
		indexConfig.setIndexName(GEO_CUSTOM_INDEX);
		indexConfig.setNumberOfShards(1);
		indexConfig.setShardCommitInterval(20);
		zuliaWorkPool.createIndex(indexConfig);

		Document doc = new Document();
		doc.put("id", "1");
		Document coords = new Document();
		coords.put("lat", NYC_LAT);
		coords.put("lng", NYC_LON);
		doc.put("coords", coords);
		Store s = new Store("1", GEO_CUSTOM_INDEX);
		s.setResultDocument(ResultDocBuilder.newBuilder().setDocument(doc));
		zuliaWorkPool.store(s);

		doc = new Document();
		doc.put("id", "2");
		coords = new Document();
		coords.put("lat", LA_LAT);
		coords.put("lng", LA_LON);
		doc.put("coords", coords);
		s = new Store("2", GEO_CUSTOM_INDEX);
		s.setResultDocument(ResultDocBuilder.newBuilder().setDocument(doc));
		zuliaWorkPool.store(s);

		Search search = new Search(GEO_CUSTOM_INDEX).setAmount(10).setRealtime(true);
		search.addQuery(new FilterQuery("coords:zl:geo(coords " + NYC_LAT + " " + NYC_LON + " 100)"));
		SearchResult result = zuliaWorkPool.search(search);
		Assertions.assertEquals(1, result.getTotalHits());

		search = new Search(GEO_CUSTOM_INDEX).setAmount(10).setRealtime(true);
		search.addSort(Sort.geoDistance("coords", NYC_LAT, NYC_LON));
		result = zuliaWorkPool.search(search);
		Assertions.assertEquals(2, result.getTotalHits());
		double firstDist = result.getCompleteResults().getFirst().getSortValues().getSortValue(0).getDoubleValue();
		Assertions.assertTrue(firstDist < 1.0, "First result should be near NYC");

		zuliaWorkPool.deleteIndex(GEO_CUSTOM_INDEX);
	}

	@Test
	@Order(10)
	public void geoJsonFormat() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD));
		indexConfig.addFieldConfig(FieldConfigBuilder.createGeoPoint("location").index());
		indexConfig.setIndexName(GEO_GEOJSON_INDEX);
		indexConfig.setNumberOfShards(1);
		indexConfig.setShardCommitInterval(20);
		zuliaWorkPool.createIndex(indexConfig);

		// GeoJSON Point format: coordinates are [lon, lat]
		Document doc = new Document();
		doc.put("id", "1");
		Document geoJson = new Document();
		geoJson.put("type", "Point");
		geoJson.put("coordinates", List.of(NYC_LON, NYC_LAT));
		doc.put("location", geoJson);
		Store s = new Store("1", GEO_GEOJSON_INDEX);
		s.setResultDocument(ResultDocBuilder.newBuilder().setDocument(doc));
		zuliaWorkPool.store(s);

		doc = new Document();
		doc.put("id", "2");
		Document loc = new Document();
		loc.put("latitude", LA_LAT);
		loc.put("longitude", LA_LON);
		doc.put("location", loc);
		s = new Store("2", GEO_GEOJSON_INDEX);
		s.setResultDocument(ResultDocBuilder.newBuilder().setDocument(doc));
		zuliaWorkPool.store(s);

		// Malformed GeoJSON (coordinates as string) — should skip indexing, not throw
		doc = new Document();
		doc.put("id", "3");
		Document badGeoJson = new Document();
		badGeoJson.put("type", "Point");
		badGeoJson.put("coordinates", "not a list");
		doc.put("location", badGeoJson);
		s = new Store("3", GEO_GEOJSON_INDEX);
		s.setResultDocument(ResultDocBuilder.newBuilder().setDocument(doc));
		zuliaWorkPool.store(s);

		Search search = new Search(GEO_GEOJSON_INDEX).setAmount(10).setRealtime(true);
		search.addQuery(new FilterQuery("location:zl:geo(location " + NYC_LAT + " " + NYC_LON + " 100)"));
		SearchResult result = zuliaWorkPool.search(search);
		Assertions.assertEquals(1, result.getTotalHits());

		// Large radius should find both valid docs but not the malformed one
		search = new Search(GEO_GEOJSON_INDEX).setAmount(10).setRealtime(true);
		search.addQuery(new FilterQuery("location:zl:geo(location " + NYC_LAT + " " + NYC_LON + " 10000)"));
		result = zuliaWorkPool.search(search);
		Assertions.assertEquals(2, result.getTotalHits());

		zuliaWorkPool.deleteIndex(GEO_GEOJSON_INDEX);
	}

	@Test
	@Order(11)
	public void storedFieldNameDiffersFromIndexAs() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD));
		indexConfig.addFieldConfig(FieldConfigBuilder.createGeoPoint("geoData").indexAsField("loc").sort());
		indexConfig.setIndexName("geoAliasTest");
		indexConfig.setNumberOfShards(1);
		indexConfig.setShardCommitInterval(20);
		zuliaWorkPool.createIndex(indexConfig);

		Document doc = new Document();
		doc.put("id", "1");
		Document geo = new Document();
		geo.put("latitude", NYC_LAT);
		geo.put("longitude", NYC_LON);
		doc.put("geoData", geo);
		Store s = new Store("1", "geoAliasTest");
		s.setResultDocument(ResultDocBuilder.newBuilder().setDocument(doc));
		zuliaWorkPool.store(s);

		Search search = new Search("geoAliasTest").setAmount(10).setRealtime(true);
		search.addQuery(new FilterQuery("loc:zl:geo(loc " + NYC_LAT + " " + NYC_LON + " 100)"));
		SearchResult result = zuliaWorkPool.search(search);
		Assertions.assertEquals(1, result.getTotalHits());

		// geodist uses the sort field name (storedFieldName for .sort()), not the index field name
		search = new Search("geoAliasTest").setAmount(10).setRealtime(true);
		search.addSort(Sort.geoDistance("geoData", NYC_LAT, NYC_LON));
		result = zuliaWorkPool.search(search);
		Assertions.assertEquals(1, result.getTotalHits());
		double dist = result.getCompleteResults().getFirst().getSortValues().getSortValue(0).getDoubleValue();
		Assertions.assertTrue(dist < 1.0, "Distance should be ~0 km");

		zuliaWorkPool.deleteIndex("geoAliasTest");
	}

	@Test
	@Order(12)
	public void topLevelLatLonFields() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// Empty storedFieldName — lat/lon at document top level
		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD));
		indexConfig.addFieldConfig(FieldConfigBuilder.createGeoPointTopLevel("lat", "lon").indexAsField("position").sortAs("position"));
		indexConfig.setIndexName(GEO_TOPLEVEL_INDEX);
		indexConfig.setNumberOfShards(1);
		indexConfig.setShardCommitInterval(20);
		zuliaWorkPool.createIndex(indexConfig);

		Document doc = new Document();
		doc.put("id", "1");
		doc.put("lat", NYC_LAT);
		doc.put("lon", NYC_LON);
		Store s = new Store("1", GEO_TOPLEVEL_INDEX);
		s.setResultDocument(ResultDocBuilder.newBuilder().setDocument(doc));
		zuliaWorkPool.store(s);

		doc = new Document();
		doc.put("id", "2");
		doc.put("lat", LA_LAT);
		doc.put("lon", LA_LON);
		s = new Store("2", GEO_TOPLEVEL_INDEX);
		s.setResultDocument(ResultDocBuilder.newBuilder().setDocument(doc));
		zuliaWorkPool.store(s);

		Search search = new Search(GEO_TOPLEVEL_INDEX).setAmount(10).setRealtime(true);
		search.addQuery(new FilterQuery("position:zl:geo(position " + NYC_LAT + " " + NYC_LON + " 100)"));
		SearchResult result = zuliaWorkPool.search(search);
		Assertions.assertEquals(1, result.getTotalHits());

		search = new Search(GEO_TOPLEVEL_INDEX).setAmount(10).setRealtime(true);
		search.addSort(Sort.geoDistance("position", NYC_LAT, NYC_LON));
		result = zuliaWorkPool.search(search);
		Assertions.assertEquals(2, result.getTotalHits());
		double firstDist = result.getCompleteResults().getFirst().getSortValues().getSortValue(0).getDoubleValue();
		Assertions.assertTrue(firstDist < 1.0, "First result should be near NYC");

		zuliaWorkPool.deleteIndex(GEO_TOPLEVEL_INDEX);
	}

	@Test
	@Order(13)
	public void sortAsWithDifferentName() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// sortAs with a different name than storedFieldName
		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD));
		indexConfig.addFieldConfig(FieldConfigBuilder.createGeoPoint("location").index().sortAs("geoSort"));
		indexConfig.setIndexName("geoSortAsTest");
		indexConfig.setNumberOfShards(2);
		indexConfig.setShardCommitInterval(20);
		zuliaWorkPool.createIndex(indexConfig);

		for (Object[] city : new Object[][] { { "1", "New York", NYC_LAT, NYC_LON }, { "2", "Los Angeles", LA_LAT, LA_LON },
				{ "3", "Chicago", CHICAGO_LAT, CHICAGO_LON }, { "4", "London", LONDON_LAT, LONDON_LON } }) {
			Document doc = new Document();
			doc.put("id", (String) city[0]);
			doc.put("name", (String) city[1]);
			Document loc = new Document();
			loc.put("latitude", (double) city[2]);
			loc.put("longitude", (double) city[3]);
			doc.put("location", loc);
			Store s = new Store((String) city[0], "geoSortAsTest");
			s.setResultDocument(ResultDocBuilder.newBuilder().setDocument(doc));
			zuliaWorkPool.store(s);
		}

		Search search = new Search("geoSortAsTest").setAmount(10).setRealtime(true);
		search.addQuery(new FilterQuery("location:zl:geo(location " + NYC_LAT + " " + NYC_LON + " 200)"));
		SearchResult result = zuliaWorkPool.search(search);
		Assertions.assertEquals(1, result.getTotalHits());

		// geodist uses sortAs name "geoSort"
		search = new Search("geoSortAsTest").setAmount(10).setRealtime(true);
		search.addSort(Sort.geoDistance("geoSort", NYC_LAT, NYC_LON));
		result = zuliaWorkPool.search(search);
		List<CompleteResult> results = result.getCompleteResults();
		Assertions.assertEquals(4, results.size());
		Assertions.assertEquals("New York", results.getFirst().getDocument().getString("name"));
		Assertions.assertEquals("London", results.getLast().getDocument().getString("name"));

		double londonDist = results.getLast().getSortValues().getSortValue(0).getDoubleValue();
		Assertions.assertTrue(londonDist > 5000 && londonDist < 6000,
				"London distance from NYC should be ~5570 km, was " + londonDist);

		search = new Search("geoSortAsTest").setAmount(10).setRealtime(true);
		search.addSort(Sort.geoDistance("geoSort", NYC_LAT, NYC_LON).descending());
		result = zuliaWorkPool.search(search);
		results = result.getCompleteResults();
		Assertions.assertEquals("London", results.getFirst().getDocument().getString("name"));
		Assertions.assertEquals("New York", results.getLast().getDocument().getString("name"));

		search = new Search("geoSortAsTest").setAmount(10).setRealtime(true);
		ScoredQuery sq = new ScoredQuery("*:*");
		sq.setScoreFunction("1.0 / (1.0 + geodist(geoSort, " + NYC_LAT + ", " + NYC_LON + "))");
		search.addQuery(sq);
		result = zuliaWorkPool.search(search);
		Assertions.assertTrue(result.getCompleteResults().getFirst().getScore() > 0.9,
				"NYC score should be ~1.0");

		zuliaWorkPool.deleteIndex("geoSortAsTest");
	}

	@Test
	@Order(14)
	public void sortOnlyWithoutIndex() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// .sort() only (no .index()) — geodist() sorting works, filtering should fail
		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD));
		indexConfig.addFieldConfig(FieldConfigBuilder.createGeoPoint("location").sort());
		indexConfig.setIndexName("geoSortOnlyTest");
		indexConfig.setNumberOfShards(1);
		indexConfig.setShardCommitInterval(20);
		zuliaWorkPool.createIndex(indexConfig);

		Document doc = new Document();
		doc.put("id", "1");
		Document loc = new Document();
		loc.put("latitude", NYC_LAT);
		loc.put("longitude", NYC_LON);
		doc.put("location", loc);
		Store s = new Store("1", "geoSortOnlyTest");
		s.setResultDocument(ResultDocBuilder.newBuilder().setDocument(doc));
		zuliaWorkPool.store(s);

		doc = new Document();
		doc.put("id", "2");
		loc = new Document();
		loc.put("latitude", LA_LAT);
		loc.put("longitude", LA_LON);
		doc.put("location", loc);
		s = new Store("2", "geoSortOnlyTest");
		s.setResultDocument(ResultDocBuilder.newBuilder().setDocument(doc));
		zuliaWorkPool.store(s);

		// geodist() sorting should work
		Search search = new Search("geoSortOnlyTest").setAmount(10).setRealtime(true);
		search.addSort(Sort.geoDistance("location", NYC_LAT, NYC_LON));
		SearchResult result = zuliaWorkPool.search(search);
		Assertions.assertEquals(2, result.getTotalHits());
		double firstDist = result.getCompleteResults().getFirst().getSortValues().getSortValue(0).getDoubleValue();
		Assertions.assertTrue(firstDist < 1.0, "First result should be near NYC");

		// Filtering should fail (no LatLonPoint BKD without .index())
		Search filterSearch = new Search("geoSortOnlyTest").setAmount(10).setRealtime(true);
		filterSearch.addQuery(new FilterQuery("location:zl:geo(location " + NYC_LAT + " " + NYC_LON + " 100)"));
		Assertions.assertThrows(Exception.class, () -> zuliaWorkPool.search(filterSearch),
				"zl:geo() should fail without .index() on field config");

		zuliaWorkPool.deleteIndex("geoSortOnlyTest");
	}

	@Test
	@Order(15)
	public void indexWithoutSort() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// .index() only (no .sort()) — filtering works, sorting/scoring should fail
		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD));
		indexConfig.addFieldConfig(FieldConfigBuilder.createGeoPoint("location").index());
		indexConfig.setIndexName("geoNoSortTest");
		indexConfig.setNumberOfShards(1);
		indexConfig.setShardCommitInterval(20);
		zuliaWorkPool.createIndex(indexConfig);

		Document doc = new Document();
		doc.put("id", "1");
		Document loc = new Document();
		loc.put("latitude", NYC_LAT);
		loc.put("longitude", NYC_LON);
		doc.put("location", loc);
		Store s = new Store("1", "geoNoSortTest");
		s.setResultDocument(ResultDocBuilder.newBuilder().setDocument(doc));
		zuliaWorkPool.store(s);

		doc = new Document();
		doc.put("id", "2");
		loc = new Document();
		loc.put("latitude", LA_LAT);
		loc.put("longitude", LA_LON);
		doc.put("location", loc);
		s = new Store("2", "geoNoSortTest");
		s.setResultDocument(ResultDocBuilder.newBuilder().setDocument(doc));
		zuliaWorkPool.store(s);

		Search search = new Search("geoNoSortTest").setAmount(10).setRealtime(true);
		search.addQuery(new FilterQuery("location:zl:geo(location " + NYC_LAT + " " + NYC_LON + " 100)"));
		SearchResult result = zuliaWorkPool.search(search);
		Assertions.assertEquals(1, result.getTotalHits());

		// geodist() sort should fail without .sort()
		Search sortSearch = new Search("geoNoSortTest").setAmount(10).setRealtime(true);
		sortSearch.addSort(Sort.geoDistance("location", NYC_LAT, NYC_LON));
		Exception sortError = Assertions.assertThrows(Exception.class, () -> zuliaWorkPool.search(sortSearch),
				"geodist() sort should fail without .sort() on field config");
		Assertions.assertTrue(sortError.getMessage().contains("not a sortable field"),
				"Error should mention not sortable, was: " + sortError.getMessage());

		// geodist() score function should also fail without .sort()
		Search scoreSearch = new Search("geoNoSortTest").setAmount(10).setRealtime(true);
		ScoredQuery sq = new ScoredQuery("*");
		sq.setScoreFunction("1.0 / (1.0 + geodist(location, " + NYC_LAT + ", " + NYC_LON + "))");
		scoreSearch.addQuery(sq);
		Exception scoreError = Assertions.assertThrows(Exception.class, () -> zuliaWorkPool.search(scoreSearch),
				"geodist() score function should fail without .sort() on field config");
		Assertions.assertTrue(scoreError.getMessage().contains("not a sortable field"),
				"Error should mention not sortable, was: " + scoreError.getMessage());

		zuliaWorkPool.deleteIndex("geoNoSortTest");
	}

	@Test
	@Order(16)
	public void errorHandling() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// geodist() on non-GEO_POINT sort field
		Search search = new Search(GEO_TEST_INDEX).setAmount(10).setRealtime(true);
		search.addSort(Sort.geoDistance("name", NYC_LAT, NYC_LON));
		Exception sortError = Assertions.assertThrows(Exception.class, () -> zuliaWorkPool.search(search));
		Assertions.assertTrue(sortError.getMessage().contains("not a GEO_POINT field"),
				"Error should mention field is not GEO_POINT, was: " + sortError.getMessage());

		// geodist() on non-existent field
		Search search2 = new Search(GEO_TEST_INDEX).setAmount(10).setRealtime(true);
		search2.addSort(Sort.geoDistance("nonexistent", NYC_LAT, NYC_LON));
		Exception fieldError = Assertions.assertThrows(Exception.class, () -> zuliaWorkPool.search(search2));
		Assertions.assertTrue(fieldError.getMessage().contains("not a sortable field"),
				"Error should mention field is not sortable, was: " + fieldError.getMessage());

		// zl:geo() on non-GEO_POINT field
		Search search3 = new Search(GEO_TEST_INDEX).setAmount(10).setRealtime(true);
		search3.addQuery(new FilterQuery("name:zl:geo(name " + NYC_LAT + " " + NYC_LON + " 100)"));
		Exception queryError = Assertions.assertThrows(Exception.class, () -> zuliaWorkPool.search(search3));
		Assertions.assertTrue(queryError.getMessage().contains("not a GEO_POINT field"),
				"Error should mention field is not GEO_POINT, was: " + queryError.getMessage());

		// GEO_POINT without .index() or .sort()
		{
			ClientIndexConfig badConfig = new ClientIndexConfig();
			badConfig.addFieldConfig(FieldConfigBuilder.createGeoPoint("location").build());
			badConfig.setIndexName("geoValidationTest");
			badConfig.setNumberOfShards(1);
			Assertions.assertThrows(Exception.class, () -> zuliaWorkPool.createIndex(badConfig), "GEO_POINT without .index() or .sort() should fail");
		}

		// GEO_POINT top-level with .index() produces empty index field name
		{
			ClientIndexConfig badConfig = new ClientIndexConfig();
			badConfig.addFieldConfig(FieldConfigBuilder.createGeoPointTopLevel("lat", "lon").index().build());
			badConfig.setIndexName("geoValidationTest2");
			badConfig.setNumberOfShards(1);
			Assertions.assertThrows(Exception.class, () -> zuliaWorkPool.createIndex(badConfig), "GEO_POINT with empty index field name should fail");
		}

		// GEO_POINT with facet
		{
			ClientIndexConfig badConfig = new ClientIndexConfig();
			badConfig.addFieldConfig(FieldConfigBuilder.createGeoPoint("location").index().facet().build());
			badConfig.setIndexName("geoValidationTest3");
			badConfig.setNumberOfShards(1);
			Assertions.assertThrows(Exception.class, () -> zuliaWorkPool.createIndex(badConfig), "GEO_POINT with .facet() should fail");
		}

		// GEO_POINT with .sort() — enables doc values for geodist() sorting/scoring
		{
			ClientIndexConfig okConfig = new ClientIndexConfig();
			okConfig.addFieldConfig(FieldConfigBuilder.createGeoPoint("location").index().sort().build());
			okConfig.setIndexName("geoValidationTest5");
			okConfig.setNumberOfShards(1);
			zuliaWorkPool.createIndex(okConfig);
			zuliaWorkPool.deleteIndex("geoValidationTest5");
		}

	}
}
