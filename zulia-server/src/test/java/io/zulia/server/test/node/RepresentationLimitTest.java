package io.zulia.server.test.node;

import io.zulia.DefaultAnalyzers;
import io.zulia.ZuliaFieldConstants;
import io.zulia.client.command.Store;
import io.zulia.client.command.builder.CountFacet;
import io.zulia.client.command.builder.FilterQuery;
import io.zulia.client.command.builder.Search;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.SearchResult;
import io.zulia.doc.ResultDocBuilder;
import io.zulia.fields.FieldConfigBuilder;
import io.zulia.message.ZuliaIndex.FieldConfig.MalformedValueHandling;
import io.zulia.message.ZuliaQuery;
import io.zulia.server.test.node.shared.NodeExtension;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.index.IndexWriter;
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
 * A value one representation cannot hold is not malformed, but it is handled the same way. Under FAIL it rejects the
 * document naming the field, and under SKIP that representation is skipped and the document marked.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class RepresentationLimitTest {

	@RegisterExtension
	static final NodeExtension nodeExtension = new NodeExtension(1);

	public static final String INDEX_NAME = "representationLimitTest";

	// the label is the facet name, a separator and the value, so a value this long is over by the name and separator
	private static final String TOO_LONG_TO_FACET = "x".repeat(FacetLabel.MAX_CATEGORY_PATH_LENGTH);
	// ascii, so bytes equal characters
	private static final String TOO_LONG_TO_SORT = "y".repeat(IndexWriter.MAX_TERM_LENGTH + 1);

	@Test
	@Order(1)
	public void createIndex() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("id");
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		// facet only and sort only, so the long value never reaches an indexed term, which has its own limit
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("strictFacet").facet());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("lenientFacet").facet().onMalformed(MalformedValueHandling.SKIP));
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("strictSort").sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("lenientSort").sort().onMalformed(MalformedValueHandling.SKIP));
		indexConfig.setIndexName(INDEX_NAME);
		indexConfig.setNumberOfShards(1);
		zuliaWorkPool.createIndex(indexConfig);
	}

	@Test
	@Order(2)
	public void tooLongToFacetIsSkippedAndMarkedUnderSkip() throws Exception {
		store("facetShort", new Document("lenientFacet", "fits"));
		store("facetLong", new Document("lenientFacet", TOO_LONG_TO_FACET));

		Assertions.assertEquals(Map.of("fits", 1L), facetCounts("lenientFacet"), "the long value has no label, the short one does");
		Assertions.assertEquals(List.of("facetLong"), ids(ZuliaFieldConstants.MALFORMED_FIELDS_LIST_FIELD + ":lenientFacet"));
		Assertions.assertEquals(1, count("id:facetlong"), "the document itself is still stored and indexed");
	}

	@Test
	@Order(3)
	public void tooLongToFacetRejectsTheDocumentUnderFail() {
		Exception e = Assertions.assertThrows(Exception.class, () -> store("facetStrict", new Document("strictFacet", TOO_LONG_TO_FACET)));
		Assertions.assertTrue(e.getMessage().contains("Field <strictFacet> facet <strictFacet> value is too long to facet"), e.getMessage());
		Assertions.assertTrue(e.getMessage().contains("<= " + FacetLabel.MAX_CATEGORY_PATH_LENGTH), e.getMessage());
	}

	@Test
	@Order(4)
	public void tooLongToSortIsSkippedAndMarkedUnderSkip() throws Exception {
		store("sortShort", new Document("lenientSort", "fits"));
		store("sortLong", new Document("lenientSort", TOO_LONG_TO_SORT));

		Assertions.assertEquals(List.of("sortLong"), ids(ZuliaFieldConstants.MALFORMED_FIELDS_LIST_FIELD + ":lenientSort"));
		Assertions.assertEquals(1, count("id:sortlong"), "the document itself is still stored and indexed");
	}

	@Test
	@Order(5)
	public void tooLongToSortRejectsTheDocumentUnderFail() {
		Exception e = Assertions.assertThrows(Exception.class, () -> store("sortStrict", new Document("strictSort", TOO_LONG_TO_SORT)));
		Assertions.assertTrue(e.getMessage().contains("Field <strictSort> sort <strictSort> value is too long to sort"), e.getMessage());
		Assertions.assertTrue(e.getMessage().contains("<= " + IndexWriter.MAX_TERM_LENGTH + " bytes"), e.getMessage());
	}

	@Test
	@Order(6)
	public void everyMarkedDocumentIsFoundByTheWildcard() throws Exception {
		Assertions.assertEquals(List.of("facetLong", "sortLong"), ids(ZuliaFieldConstants.MALFORMED_FIELDS_LIST_FIELD + ":*"));
	}

	private static void store(String id, Document document) throws Exception {
		document.put("id", id);
		nodeExtension.getClient().store(new Store(id, INDEX_NAME, ResultDocBuilder.from(document)));
	}

	private static long count(String query) throws Exception {
		return nodeExtension.getClient().search(new Search(INDEX_NAME).setRealtime(true).setAmount(0).addQuery(new FilterQuery(query))).getTotalHits();
	}

	private static List<String> ids(String query) throws Exception {
		Search search = new Search(INDEX_NAME).setRealtime(true).setAmount(100).addQuery(new FilterQuery(query));
		return nodeExtension.getClient().search(search).getUniqueIds().stream().sorted().toList();
	}

	private static Map<String, Long> facetCounts(String facet) throws Exception {
		SearchResult searchResult = nodeExtension.getClient().search(new Search(INDEX_NAME).setRealtime(true).addCountFacet(new CountFacet(facet)));
		return searchResult.getFacetCounts(facet).stream().collect(Collectors.toMap(ZuliaQuery.FacetCount::getFacet, ZuliaQuery.FacetCount::getCount));
	}
}
