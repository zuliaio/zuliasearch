package io.zulia.server.test.node;

import io.zulia.DefaultAnalyzers;
import io.zulia.client.command.Store;
import io.zulia.client.command.builder.FilterQuery;
import io.zulia.client.command.builder.NumericSetQuery;
import io.zulia.client.command.builder.Search;
import io.zulia.client.command.builder.Sort;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.SearchResult;
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

import java.util.Arrays;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class NumericSetTest {

	@RegisterExtension
	static final NodeExtension nodeExtension = new NodeExtension(3);

	public static final String NUMERIC_SET_TEST = "nsTest";

	private static final int uniqueDocs = 6;

	@Test
	@Order(1)
	public void createIndex() throws Exception {

		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("title");
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createInt("intField").index().sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createLong("longField").index());
		indexConfig.addFieldConfig(FieldConfigBuilder.createFloat("floatField").index());
		indexConfig.addFieldConfig(FieldConfigBuilder.createDouble("doubleField").index());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("fn").indexAs(DefaultAnalyzers.STANDARD));
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("zl").indexAs(DefaultAnalyzers.STANDARD));
		indexConfig.setIndexName(NUMERIC_SET_TEST);
		indexConfig.setNumberOfShards(1);
		indexConfig.setShardCommitInterval(20); //force some commits

		zuliaWorkPool.createIndex(indexConfig);
	}

	@Test
	@Order(2)
	public void index() throws Exception {

		indexRecord(1, 1, 1L, 1.0f, 1.0);
		indexRecord(2, 2, 23232323L, 132.0f, 2.0);
		indexRecord(3, 52, 5L, 565.0f, 2444.0);
		indexRecord(4, 12332, 3323232323L, 4.0f, 2.01);
		indexRecord(5, 2, 1L, 2000f, 2.0);
		indexRecord(6, 10, 5L, 1.2f, 2.001);

	}

	private void indexRecord(int id, int i, long l, float f, double d) throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		String uniqueId = String.valueOf(id);

		Document mongoDocument = new Document();
		mongoDocument.put("id", uniqueId);
		mongoDocument.put("intField", i);
		mongoDocument.put("longField", l);
		mongoDocument.put("floatField", f);
		mongoDocument.put("doubleField", d);

		if (id == 1) {
			mongoDocument.put("fn", "ordered");
		}

		if (id == 2) {
			mongoDocument.put("zl", "ns");
		}

		Store s = new Store(uniqueId, NUMERIC_SET_TEST);

		ResultDocBuilder resultDocumentBuilder = ResultDocBuilder.newBuilder().setDocument(mongoDocument);
		s.setResultDocument(resultDocumentBuilder);
		zuliaWorkPool.store(s);

	}

	@Test
	@Order(3)
	public void searchTest() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		Search search = new Search(NUMERIC_SET_TEST);
		SearchResult searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(uniqueDocs, searchResult.getTotalHits());

		search = new Search(NUMERIC_SET_TEST);
		search.addQuery(new NumericSetQuery("intField").addValues(1, 2)).addSort(new Sort("id"));
		search.setAmount(10);
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(3, searchResult.getTotalHits());

		Assertions.assertEquals("1", searchResult.getCompleteResults().get(0).getUniqueId());
		Assertions.assertEquals("2", searchResult.getCompleteResults().get(1).getUniqueId());
		Assertions.assertEquals("5", searchResult.getCompleteResults().get(2).getUniqueId());

		search = new Search(NUMERIC_SET_TEST);
		search.addQuery(new NumericSetQuery("intField").addIntValues(Arrays.asList(1, 2))).addSort(new Sort("id"));
		search.setAmount(10);
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(3, searchResult.getTotalHits());

		Assertions.assertEquals("1", searchResult.getCompleteResults().get(0).getUniqueId());
		Assertions.assertEquals("2", searchResult.getCompleteResults().get(1).getUniqueId());
		Assertions.assertEquals("5", searchResult.getCompleteResults().get(2).getUniqueId());

		search = new Search(NUMERIC_SET_TEST);
		search.addQuery(new FilterQuery("intField:zl:ns(1 2)")).addSort(new Sort("id"));
		search.setAmount(10);
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(3, searchResult.getTotalHits());

		Assertions.assertEquals("1", searchResult.getCompleteResults().get(0).getUniqueId());
		Assertions.assertEquals("2", searchResult.getCompleteResults().get(1).getUniqueId());
		Assertions.assertEquals("5", searchResult.getCompleteResults().get(2).getUniqueId());

		search = new Search(NUMERIC_SET_TEST);
		search.addQuery(new FilterQuery("zl:ns(1 2)").addQueryField("intField")).addSort(new Sort("id"));
		search.setAmount(10);
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(3, searchResult.getTotalHits());

		Assertions.assertEquals("1", searchResult.getCompleteResults().get(0).getUniqueId());
		Assertions.assertEquals("2", searchResult.getCompleteResults().get(1).getUniqueId());
		Assertions.assertEquals("5", searchResult.getCompleteResults().get(2).getUniqueId());

		//IllegalArgumentException:Search: For input string: "abcd"
		Assertions.assertThrows(Exception.class, () -> {
			Search s = new Search(NUMERIC_SET_TEST);
			s.addQuery(new FilterQuery("zl:ns(1 abcd)").addQueryField("intField")).addSort(new Sort("id"));
			s.setAmount(10);
			zuliaWorkPool.search(s);
		});

		//Exception:Search: Field <title> must be indexed for numeric set queries
		Assertions.assertThrows(Exception.class, () -> {
			Search s = new Search(NUMERIC_SET_TEST);
			s.addQuery(new FilterQuery("zl:ns(1 2)").addQueryField("title")).addSort(new Sort("id"));
			s.setAmount(10);
			zuliaWorkPool.search(s);
		});

		search = new Search(NUMERIC_SET_TEST);
		search.addQuery(new FilterQuery("intField,longField:zl:ns(1 2 5)")).addSort(new Sort("id"));
		search.setAmount(10);
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(5, searchResult.getTotalHits());

		search = new Search(NUMERIC_SET_TEST);
		search.addQuery(new FilterQuery("intField,longField:zl:ns(1 2 5) -id:1")).addSort(new Sort("id"));
		search.setAmount(10);
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(4, searchResult.getTotalHits());

		search = new Search(NUMERIC_SET_TEST);
		search.addQuery(new FilterQuery("zl:ns(1 2 5)").addQueryFields("intField", "longField")).addSort(new Sort("id"));
		search.setAmount(10);
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(5, searchResult.getTotalHits());

		search = new Search(NUMERIC_SET_TEST);
		search.addQuery(new NumericSetQuery("longField").addValues(1L));
		search.setAmount(10);
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(2, searchResult.getTotalHits());

		search = new Search(NUMERIC_SET_TEST);
		search.addQuery(new NumericSetQuery("floatField").addValues(565.0f, 2000f));
		search.setAmount(10);
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(2, searchResult.getTotalHits());

		search = new Search(NUMERIC_SET_TEST);
		search.addQuery(new FilterQuery("zl:ns(565.0 2000)").addQueryField("floatField")).addSort(new Sort("id"));
		search.setAmount(10);
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(2, searchResult.getTotalHits());

		search = new Search(NUMERIC_SET_TEST);
		search.addQuery(new NumericSetQuery("doubleField").addValues(2.01, 2.0));
		search.setAmount(3);
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(3, searchResult.getTotalHits());

		search = new Search(NUMERIC_SET_TEST);
		search.addQuery(new FilterQuery("doubleField:zl:ns(2.01 2.0)")).addSort(new Sort("id"));
		search.setAmount(10);
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(3, searchResult.getTotalHits());

		// to query fn or zl, need to use multi-field syntax trick to query or use query fields

		search = new Search(NUMERIC_SET_TEST);
		search.addQuery(new FilterQuery("ordered").addQueryFields("fn"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(1, searchResult.getTotalHits());

		search = new Search(NUMERIC_SET_TEST);
		search.addQuery(new FilterQuery("fn,:ordered"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(1, searchResult.getTotalHits());

		search = new Search(NUMERIC_SET_TEST);
		search.addQuery(new FilterQuery("ns").addQueryFields("zl"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(1, searchResult.getTotalHits());

		search = new Search(NUMERIC_SET_TEST);
		search.addQuery(new FilterQuery("zl,:ns"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(1, searchResult.getTotalHits());

	}

	@Test
	@Order(5)
	public void restart() throws Exception {
		nodeExtension.restartNodes();
	}

	@Test
	@Order(6)
	public void confirm() throws Exception {
		searchTest();
	}

}
