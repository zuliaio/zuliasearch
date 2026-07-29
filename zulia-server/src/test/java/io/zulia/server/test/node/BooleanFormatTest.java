package io.zulia.server.test.node;

import io.zulia.DefaultAnalyzers;
import io.zulia.client.command.Store;
import io.zulia.client.command.builder.CountFacet;
import io.zulia.client.command.builder.FilterQuery;
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
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A boolean field accepts the native boolean, the textual forms true, t, yes, y, 1 and false, f, no, n, 0 case
 * insensitively, and the numbers 1 and 0. The indexed value, the sort value, and the facet label must all agree on
 * what a given value means, and anything outside that set must fail the store instead of being coerced.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class BooleanFormatTest {

	@RegisterExtension
	static final NodeExtension nodeExtension = new NodeExtension(1);

	public static final String INDEX_NAME = "booleanFormatTest";

	private static final List<Object> TRUE_VALUES = List.of(Boolean.TRUE, "true", "TRUE", "T", "yes", "Yes", "y", "1", 1, 1L, 1.0d);
	private static final List<Object> FALSE_VALUES = List.of(Boolean.FALSE, "false", "FALSE", "F", "no", "No", "n", "0", 0, 0L, 0.0d);

	@Test
	@Order(1)
	public void createIndex() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("id");
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createBool("flag").index().facet().sort());
		indexConfig.setIndexName(INDEX_NAME);
		indexConfig.setNumberOfShards(1);

		zuliaWorkPool.createIndex(indexConfig);
	}

	@Test
	@Order(2)
	public void index() throws Exception {
		for (int i = 0; i < TRUE_VALUES.size(); i++) {
			store("true-" + i, TRUE_VALUES.get(i));
		}
		for (int i = 0; i < FALSE_VALUES.size(); i++) {
			store("false-" + i, FALSE_VALUES.get(i));
		}
	}

	@Test
	@Order(3)
	public void everyFormatQueriesAsTheSameValue() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		Search trueSearch = new Search(INDEX_NAME).setRealtime(true).addQuery(new FilterQuery("flag:true"));
		Assertions.assertEquals(TRUE_VALUES.size(), zuliaWorkPool.search(trueSearch).getTotalHits());

		Search falseSearch = new Search(INDEX_NAME).setRealtime(true).addQuery(new FilterQuery("flag:false"));
		Assertions.assertEquals(FALSE_VALUES.size(), zuliaWorkPool.search(falseSearch).getTotalHits());
	}

	@Test
	@Order(4)
	public void facetLabelsAgreeWithTheIndexedValue() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// a double 1.0 used to facet as nothing at all because the facet path stringified it to "1.0" first,
		// while the same value indexed as true, so the facet counts disagreed with the query counts
		SearchResult searchResult = zuliaWorkPool.search(new Search(INDEX_NAME).setRealtime(true).addCountFacet(new CountFacet("flag")));

		Map<String, Long> counts = searchResult.getFacetCounts("flag").stream()
				.collect(Collectors.toMap(ZuliaQuery.FacetCount::getFacet, ZuliaQuery.FacetCount::getCount));

		Assertions.assertEquals(Long.valueOf(TRUE_VALUES.size()), counts.get("True"));
		Assertions.assertEquals(Long.valueOf(FALSE_VALUES.size()), counts.get("False"));
	}

	@Test
	@Order(5)
	public void valuesOutsideTheAcceptedSetAreRejected() {
		// 1.5 truncated to 1 and indexed as true, and 2^32 wrapped to 0 and indexed as false, because the
		// number branch compared intValue() instead of the actual value
		Assertions.assertThrows(Exception.class, () -> store("bad-fraction", 1.5d));
		Assertions.assertThrows(Exception.class, () -> store("bad-wrap", 4294967296L));
		Assertions.assertThrows(Exception.class, () -> store("bad-number", 2));
		Assertions.assertThrows(Exception.class, () -> store("bad-string", "maybe"));
	}

	private static void store(String id, Object flag) throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		Document document = new Document();
		document.put("id", id);
		document.put("flag", flag);

		zuliaWorkPool.store(new Store(id, INDEX_NAME, ResultDocBuilder.from(document)));
	}
}
