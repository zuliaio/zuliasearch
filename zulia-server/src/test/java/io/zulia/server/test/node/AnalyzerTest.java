package io.zulia.server.test.node;

import io.zulia.DefaultAnalyzers;
import io.zulia.client.command.Store;
import io.zulia.client.command.builder.ScoredQuery;
import io.zulia.client.command.builder.Search;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.SearchResult;
import io.zulia.doc.ResultDocBuilder;
import io.zulia.fields.FieldConfigBuilder;
import io.zulia.message.ZuliaIndex.AnalyzerSettings;
import io.zulia.message.ZuliaIndex.AnalyzerSettings.Filter;
import io.zulia.server.test.node.shared.NodeExtension;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.RegisterExtension;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class AnalyzerTest {

	@RegisterExtension
	static final NodeExtension nodeExtension = new NodeExtension(3);

	public static final String ANALYZER_TEST_INDEX = "analyzerTest";

	private static final int repeatCount = 50;
	private static final int uniqueDocs = 3;

	@Test
	@Order(1)
	public void createIndex() throws Exception {

		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		ClientIndexConfig indexConfig = new ClientIndexConfig();

		indexConfig.addAnalyzerSetting(
				AnalyzerSettings.newBuilder().setName("myAnalyzer").setTokenizer(AnalyzerSettings.Tokenizer.STANDARD).addFilter(Filter.LOWERCASE)
						.addFilter(Filter.ASCII_FOLDING).addFilter(Filter.GERMAN_NORMALIZATION).addFilter(Filter.ENGLISH_POSSESSIVE)
						.addFilter(Filter.ENGLISH_MIN_STEM).addFilter(Filter.BRITISH_US).build());

		indexConfig.addDefaultSearchField("title");
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		indexConfig.addFieldConfig(
				FieldConfigBuilder.createString("title").indexAs(DefaultAnalyzers.STANDARD, "titleStandard").indexAs("myAnalyzer", "titleCustom").sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("description").indexAs("myAnalyzer").sort());
		indexConfig.setIndexName(ANALYZER_TEST_INDEX);
		indexConfig.setNumberOfShards(1);
		indexConfig.setShardCommitInterval(20); //force some commits

		zuliaWorkPool.createIndex(indexConfig);
	}

	@Test
	@Order(2)
	public void index() throws Exception {

		for (int i = 0; i < repeatCount; i++) {
			// random wikipedia titles and first bit of text from german and hungarian wikipedia to test folding
			indexRecord(i * uniqueDocs, "Jürgen",
					"Jürgen ist eine deutsche Nebenform des männlichen Vornamens Georg – Widmungen an den Heiligen Georg finden sich daher auch unter St. Jürgen.");
			indexRecord(i * uniqueDocs + 1, "Straße ",
					"Eine Straße ist im Landverkehr ein Verkehrsbauwerk, das Fußgängern und Fahrzeugen als Transport- und Verkehrsweg überwiegend dem Personentransport, dem Gütertransport und dem Tiertransport zur Ortsveränderung dient.");
			indexRecord(i * uniqueDocs + 2, "András",
					"Az András[1] a görög Andreasz (Ανδρέας) névből származó férfinév. Jelentése: férfi, férfias. Az András bibliai név, András apostol Jézus első tanítványa, Szent Péter fivére volt.[2] Női párja: Andrea.");
		}

	}

	private void indexRecord(int id, String title, String description) throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		String uniqueId = String.valueOf(id);

		Document mongoDocument = new Document();
		mongoDocument.put("id", uniqueId);
		mongoDocument.put("title", title);
		mongoDocument.put("description", description);

		Store s = new Store(uniqueId, ANALYZER_TEST_INDEX);

		ResultDocBuilder resultDocumentBuilder = ResultDocBuilder.newBuilder().setDocument(mongoDocument);
		s.setResultDocument(resultDocumentBuilder);
		zuliaWorkPool.store(s);

	}

	@Test
	@Order(3)
	public void searchTest() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		Search search = new Search(ANALYZER_TEST_INDEX);
		SearchResult searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(repeatCount * uniqueDocs, searchResult.getTotalHits());

		//
		search = new Search(ANALYZER_TEST_INDEX);
		search.addQuery(new ScoredQuery("titleCustom:Jürgen"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(repeatCount, searchResult.getTotalHits());

		search = new Search(ANALYZER_TEST_INDEX);
		search.addQuery(new ScoredQuery("titleCustom:Juergen"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(repeatCount, searchResult.getTotalHits());

		search = new Search(ANALYZER_TEST_INDEX);
		search.addQuery(new ScoredQuery("titleStandard:Jürgen"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(repeatCount, searchResult.getTotalHits());

		search = new Search(ANALYZER_TEST_INDEX);
		search.addQuery(new ScoredQuery("titleStandard:Juergen"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(0, searchResult.getTotalHits());

		search = new Search(ANALYZER_TEST_INDEX);
		search.addQuery(new ScoredQuery("description:Straße"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(repeatCount, searchResult.getTotalHits());

		search = new Search(ANALYZER_TEST_INDEX);
		search.addQuery(new ScoredQuery("description:Strasse"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(repeatCount, searchResult.getTotalHits());

		search = new Search(ANALYZER_TEST_INDEX);
		search.addQuery(new ScoredQuery("description:Fussgängern"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(repeatCount, searchResult.getTotalHits());

		search = new Search(ANALYZER_TEST_INDEX);
		search.addQuery(new ScoredQuery("András").addQueryFields("titleStandard"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(repeatCount, searchResult.getTotalHits());

		search = new Search(ANALYZER_TEST_INDEX);
		search.addQuery(new ScoredQuery("andras").addQueryFields("titleStandard"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(0, searchResult.getTotalHits());

		search = new Search(ANALYZER_TEST_INDEX);
		search.addQuery(new ScoredQuery("András").addQueryFields("titleCustom"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(repeatCount, searchResult.getTotalHits());

		search = new Search(ANALYZER_TEST_INDEX);
		search.addQuery(new ScoredQuery("andras").addQueryFields("titleCustom"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(repeatCount, searchResult.getTotalHits());

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
