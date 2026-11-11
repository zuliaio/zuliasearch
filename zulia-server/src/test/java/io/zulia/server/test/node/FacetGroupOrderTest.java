package io.zulia.server.test.node;

import io.zulia.DefaultAnalyzers;
import io.zulia.client.command.Store;
import io.zulia.client.command.builder.CountFacet;
import io.zulia.client.command.builder.NumericStat;
import io.zulia.client.command.builder.Search;
import io.zulia.client.command.builder.StatFacet;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.SearchResult;
import io.zulia.doc.ResultDocBuilder;
import io.zulia.fields.FieldConfigBuilder;
import io.zulia.message.ZuliaQuery.StatRequest;
import io.zulia.server.test.node.shared.NodeExtension;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;

/** Facet and stat groups come back in the order they were requested. */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class FacetGroupOrderTest {

	@RegisterExtension
	static final NodeExtension nodeExtension = new NodeExtension(3);

	private static final String INDEX_NAME = "facetGroupOrder";

	// not alphabetical, so a sorted response does not pass by accident
	private static final List<String> FACETS = List.of("shape", "brand", "origin", "color", "grade", "size", "vendor", "aisle");
	private static final List<String> NUMBERS = List.of("weight", "price", "rating", "stock");

	@Test
	@Order(1)
	public void createIndex() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("id");
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		for (String facet : FACETS) {
			indexConfig.addFieldConfig(FieldConfigBuilder.createString(facet).indexAs(DefaultAnalyzers.LC_KEYWORD).facet());
		}
		for (String number : NUMBERS) {
			indexConfig.addFieldConfig(FieldConfigBuilder.createDouble(number).index().sort());
		}
		indexConfig.setIndexName(INDEX_NAME);
		indexConfig.setNumberOfShards(4);
		zuliaWorkPool.createIndex(indexConfig);
	}

	@Test
	@Order(2)
	public void index() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		for (int i = 0; i < 40; i++) {
			Document doc = new Document();
			doc.put("id", "doc-" + i);
			for (String facet : FACETS) {
				doc.put(facet, facet + "-" + (i % 3));
			}
			for (String number : NUMBERS) {
				doc.put(number, (double) i);
			}
			Store store = new Store("doc-" + i, INDEX_NAME);
			store.setResultDocument(ResultDocBuilder.from(doc));
			zuliaWorkPool.store(store);
		}
	}

	@Test
	@Order(3)
	public void countFacetGroupsFollowTheRequest() throws Exception {
		Search search = new Search(INDEX_NAME).setRealtime(true);
		for (String facet : FACETS) {
			search.addCountFacet(new CountFacet(facet));
		}
		Assertions.assertEquals(FACETS, facetGroupLabels(nodeExtension.getClient().search(search)));
	}

	@Test
	@Order(4)
	public void repeatedCountFacetKeepsItsFirstPlace() throws Exception {
		Search search = new Search(INDEX_NAME).setRealtime(true);
		for (String facet : FACETS) {
			search.addCountFacet(new CountFacet(facet));
		}
		// the validator folds a repeat into the first request for that facet
		search.addCountFacet(new CountFacet("brand").setTopN(2));
		Assertions.assertEquals(FACETS, facetGroupLabels(nodeExtension.getClient().search(search)));
	}

	@Test
	@Order(5)
	public void statGroupsFollowTheRequest() throws Exception {
		Search search = new Search(INDEX_NAME).setRealtime(true);
		List<String> expected = List.of("weight", "price by shape", "rating", "price by aisle", "stock by brand", "price", "weight by color",
				"stock", "rating by origin", "weight by size");
		for (String stat : expected) {
			String[] parts = stat.split(" by ");
			if (parts.length == 1) {
				search.addStat(new NumericStat(parts[0]));
			}
			else {
				search.addStat(new StatFacet(parts[0], parts[1]));
			}
		}
		SearchResult result = nodeExtension.getClient().search(search);
		List<String> actual = result.getStatGroups().stream().map(group -> statName(group.getStatRequest())).toList();
		Assertions.assertEquals(expected, actual);
	}

	private static List<String> facetGroupLabels(SearchResult result) {
		return result.getFacetGroups().stream().map(group -> group.getCountRequest().getFacetField().getLabel()).toList();
	}

	private static String statName(StatRequest statRequest) {
		String facet = statRequest.getFacetField().getLabel();
		return facet.isEmpty() ? statRequest.getNumericField() : statRequest.getNumericField() + " by " + facet;
	}
}
