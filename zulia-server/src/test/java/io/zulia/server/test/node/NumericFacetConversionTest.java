package io.zulia.server.test.node;

import io.zulia.client.command.Store;
import io.zulia.client.command.builder.CountFacet;
import io.zulia.client.command.builder.NumericSetQuery;
import io.zulia.client.command.builder.Search;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
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

/**
 * Regression test for facet labels on integral numeric fields.
 * A NUMERIC_INT or NUMERIC_LONG field commonly receives a floating point value. A Double 2020.0 is
 * the usual JSON and Mongo encoding of a whole number. The index and sort paths conversion that value to
 * its declared integral type, so the facet label must agree and read "2020", not "2020.0". A Double
 * and an Integer of the same value must collapse into a single facet bucket, and the label must match
 * the value as it was actually indexed.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class NumericFacetConversionTest {

	@RegisterExtension
	static final NodeExtension nodeExtension = new NodeExtension(1);

	private static final String INDEX = "numericFacetCoercionTest";

	@Test
	@Order(1)
	public void createIndex() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("title");
		indexConfig.addFieldConfig(FieldConfigBuilder.createInt("year").index().facet().sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createLong("bigNum").index().facet().sort());
		indexConfig.setIndexName(INDEX);
		indexConfig.setNumberOfShards(1);

		zuliaWorkPool.createIndex(indexConfig);
	}

	@Test
	@Order(2)
	public void index() throws Exception {
		// A Double encoding of an integral value, plus a matching Integer/Long, so we can prove they collapse to one bucket.
		store("1", 2020.0, 10_000_000_000.0);
		store("2", 2020, 10_000_000_000L);
		store("3", 2021.0, 20_000_000_000.0);
	}

	private void store(String id, Object year, Object bigNum) throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		Document doc = new Document();
		doc.put("id", id);
		doc.put("year", year);
		doc.put("bigNum", bigNum);

		Store store = new Store(id, INDEX);
		store.setResultDocument(ResultDocBuilder.newBuilder().setDocument(doc));
		zuliaWorkPool.store(store);
	}

	@Test
	@Order(3)
	public void facetLabelsAreIntegral() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		Search search = new Search(INDEX).setRealtime(true).addCountFacet(new CountFacet("year")).addCountFacet(new CountFacet("bigNum"));
		SearchResult searchResult = zuliaWorkPool.search(search);

		Assertions.assertEquals(3, searchResult.getTotalHits());

		// The Double 2020.0 and the Integer 2020 collapse into a single "2020" bucket. No "2020.0" label exists.
		List<ZuliaQuery.FacetCount> yearCounts = searchResult.getFacetCounts("year");
		Assertions.assertEquals(2L, countFor(yearCounts, "2020"));
		Assertions.assertEquals(1L, countFor(yearCounts, "2021"));
		Assertions.assertTrue(yearCounts.stream().noneMatch(fc -> fc.getFacet().contains(".")), "No year facet label should contain a decimal point: " + yearCounts);

		// Same coercion for NUMERIC_LONG.
		List<ZuliaQuery.FacetCount> bigNumCounts = searchResult.getFacetCounts("bigNum");
		Assertions.assertEquals(2L, countFor(bigNumCounts, "10000000000"));
		Assertions.assertEquals(1L, countFor(bigNumCounts, "20000000000"));
		Assertions.assertTrue(bigNumCounts.stream().noneMatch(fc -> fc.getFacet().contains(".")),
				"No bigNum facet label should contain a decimal point: " + bigNumCounts);
	}

	@Test
	@Order(4)
	public void facetLabelRoundTripsToNumericQuery() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// The "2020" label must match the value as it was actually indexed, proving the facet and index paths agree.
		Search yearSearch = new Search(INDEX).setRealtime(true).addQuery(new NumericSetQuery("year").addValues(2020));
		Assertions.assertEquals(2, zuliaWorkPool.search(yearSearch).getTotalHits());

		Search bigNumSearch = new Search(INDEX).setRealtime(true).addQuery(new NumericSetQuery("bigNum").addValues(10_000_000_000L));
		Assertions.assertEquals(2, zuliaWorkPool.search(bigNumSearch).getTotalHits());
	}

	private static long countFor(List<ZuliaQuery.FacetCount> counts, String facet) {
		return counts.stream().filter(fc -> fc.getFacet().equals(facet)).mapToLong(ZuliaQuery.FacetCount::getCount).findFirst().orElse(0L);
	}

}
