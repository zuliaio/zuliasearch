package io.zulia.server.test.node;

import io.zulia.DefaultAnalyzers;
import io.zulia.ZuliaRESTConstants;
import io.zulia.client.command.Store;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.rest.ZuliaRESTClient;
import io.zulia.client.rest.options.SearchREST;
import io.zulia.client.rest.options.TermsRESTOptions;
import io.zulia.fields.FieldConfigBuilder;
import io.zulia.message.ZuliaServiceOuterClass.RestIndexSettingsResponse;
import io.zulia.rest.dto.AssociatedMetadataDTO;
import io.zulia.rest.dto.FacetDTO;
import io.zulia.rest.dto.FacetsDTO;
import io.zulia.rest.dto.FieldsDTO;
import io.zulia.rest.dto.HighlightDTO;
import io.zulia.rest.dto.IndexMappingDTO;
import io.zulia.rest.dto.IndexesResponseDTO;
import io.zulia.rest.dto.NodeDTO;
import io.zulia.rest.dto.NodesResponseDTO;
import io.zulia.rest.dto.ScoredResultDTO;
import io.zulia.rest.dto.SearchResultsDTO;
import io.zulia.rest.dto.StatsDTO;
import io.zulia.rest.dto.TermsResponseDTO;
import io.zulia.server.test.node.shared.RestNodeExtension;
import io.zulia.util.ZuliaVersion;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class RestTest {

	@RegisterExtension
	static final RestNodeExtension restNodeExtension = new RestNodeExtension(1);

	@Test
	@Order(1)
	public void init() throws Exception {
		ZuliaWorkPool zuliaWorkPool = restNodeExtension.getGrpcClient();
		{
			ClientIndexConfig indexConfig = new ClientIndexConfig();
			indexConfig.addDefaultSearchField("title");
			indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
			indexConfig.addFieldConfig(FieldConfigBuilder.createString("title").indexAs(DefaultAnalyzers.STANDARD).sort());
			indexConfig.addFieldConfig(FieldConfigBuilder.createInt("year").facet().sort());
			indexConfig.setIndexName("index1");
			indexConfig.setDisableCompression(true);
			indexConfig.setNumberOfShards(1);

			zuliaWorkPool.createIndex(indexConfig);
		}

		{
			ClientIndexConfig indexConfig = new ClientIndexConfig();
			indexConfig.addDefaultSearchField("title");
			indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
			indexConfig.addFieldConfig(FieldConfigBuilder.createString("title").indexAs(DefaultAnalyzers.STANDARD));
			indexConfig.setIndexName("index2");
			indexConfig.setDisableCompression(true);
			indexConfig.setNumberOfShards(1);

			zuliaWorkPool.createIndex(indexConfig);
		}

		{
			Store store = new Store("123", "index1");
			store.setResultDocument(new Document("id", "123").append("title", "test").append("notIndexed", "some value").append("year", 2022));
			zuliaWorkPool.store(store);
		}

		{
			Store store = new Store("456", "index1");
			store.setResultDocument(new Document("id", "456").append("title", "some value").append("notIndexed", "the best value").append("year", 2022));
			zuliaWorkPool.store(store);
		}

		{
			Store store = new Store("789", "index1");
			store.setResultDocument(
					new Document("id", "789").append("title", "a totally different place and time").append("notIndexed", "random stuff").append("year", 2021));
			zuliaWorkPool.store(store);
		}

	}

	@Test
	@Order(2)
	public void indexesTest() {
		ZuliaRESTClient restClient = restNodeExtension.getRESTClient();
		IndexesResponseDTO indexes = restClient.getIndexes();
		Assertions.assertEquals(2, indexes.getIndexes().size());
	}

	@Test
	@Order(3)
	public void indexTest() throws Exception {
		ZuliaRESTClient restClient = restNodeExtension.getRESTClient();
		RestIndexSettingsResponse restIndexSettingsResponse = restClient.getIndex("index1");
		Assertions.assertEquals(3, restIndexSettingsResponse.getIndexSettings().getFieldConfigList().size());
		Assertions.assertEquals("id", restIndexSettingsResponse.getIndexSettings().getFieldConfigList().get(0).getStoredFieldName());
		Assertions.assertEquals("title", restIndexSettingsResponse.getIndexSettings().getFieldConfigList().get(1).getStoredFieldName());
		Assertions.assertEquals("year", restIndexSettingsResponse.getIndexSettings().getFieldConfigList().get(2).getStoredFieldName());
	}

	@Test
	@Order(4)
	public void nodesTest() throws Exception {
		ZuliaRESTClient restClient = restNodeExtension.getRESTClient();
		NodesResponseDTO nodesResponseDTO = restClient.getNodes(true);
		List<NodeDTO> members = nodesResponseDTO.getMembers();
		Assertions.assertEquals(1, members.size());
		NodeDTO member = members.getFirst();
		Assertions.assertEquals(20001, member.getServicePort());
		Assertions.assertEquals(20002, member.getRestPort());
		Assertions.assertEquals("localhost", member.getServerAddress());

		List<IndexMappingDTO> memberIndexMappings = member.getIndexMappings();
		Assertions.assertEquals(2, memberIndexMappings.size());
		Assertions.assertEquals("index1", memberIndexMappings.getFirst().getName());

	}

	@Test
	@Order(5)
	public void fieldsTest() {
		ZuliaRESTClient restClient = restNodeExtension.getRESTClient();
		FieldsDTO fields = restClient.getFields("index1");

		Assertions.assertEquals(2, fields.getFields().size());
		Assertions.assertTrue(fields.getFields().contains("title"));
		Assertions.assertTrue(fields.getFields().contains("id"));

		fields = restClient.getFields("index2");

		Assertions.assertEquals(0, fields.getFields().size());

	}

	@Test
	@Order(5)
	public void fetchTest() {
		ZuliaRESTClient restClient = restNodeExtension.getRESTClient();
		Document document = restClient.fetchRecord("index1", "123");
		Assertions.assertEquals("test", document.getString("title"));
		Assertions.assertEquals("some value", document.getString("notIndexed"));
	}

	@Test
	@Order(5)
	public void statTest() {
		ZuliaRESTClient restClient = restNodeExtension.getRESTClient();
		StatsDTO statsDTO = restClient.getStats();
		Assertions.assertEquals(statsDTO.getZuliaVersion(), ZuliaVersion.getVersion());
	}

	@Test
	@Order(6)
	public void termTest() {
		ZuliaRESTClient restClient = restNodeExtension.getRESTClient();
		TermsResponseDTO termsResponseDTO = restClient.getTerms("index1", "title");
		Assertions.assertEquals(7, termsResponseDTO.getTerms().size());
		Assertions.assertEquals("different", termsResponseDTO.getTerms().get(0).term());
		Assertions.assertEquals("place", termsResponseDTO.getTerms().get(1).term());
		Assertions.assertEquals("some", termsResponseDTO.getTerms().get(2).term());
		Assertions.assertEquals("test", termsResponseDTO.getTerms().get(3).term());
		Assertions.assertEquals("time", termsResponseDTO.getTerms().get(4).term());
		Assertions.assertEquals("totally", termsResponseDTO.getTerms().get(5).term());
		Assertions.assertEquals("value", termsResponseDTO.getTerms().get(6).term());

		termsResponseDTO = restClient.getTerms("index1", "title", new TermsRESTOptions().setAmount(1));
		Assertions.assertEquals(1, termsResponseDTO.getTerms().size());
		Assertions.assertEquals("different", termsResponseDTO.getTerms().get(0).term());

		termsResponseDTO = restClient.getTerms("index1", "title", new TermsRESTOptions().setAmount(1).setStartTerm("t"));
		Assertions.assertEquals(1, termsResponseDTO.getTerms().size());
		Assertions.assertEquals("test", termsResponseDTO.getTerms().get(0).term());

		termsResponseDTO = restClient.getTerms("index1", "title", new TermsRESTOptions().setTermFilter("test"));
		Assertions.assertEquals(6, termsResponseDTO.getTerms().size());
		Assertions.assertEquals("different", termsResponseDTO.getTerms().get(0).term());
		Assertions.assertEquals("place", termsResponseDTO.getTerms().get(1).term());
		Assertions.assertEquals("some", termsResponseDTO.getTerms().get(2).term());
		Assertions.assertEquals("time", termsResponseDTO.getTerms().get(3).term());
		Assertions.assertEquals("totally", termsResponseDTO.getTerms().get(4).term());
		Assertions.assertEquals("value", termsResponseDTO.getTerms().get(5).term());

	}

	@Test
	@Order(6)
	public void filesTest() throws Exception {
		ZuliaRESTClient restClient = restNodeExtension.getRESTClient();

		byte[] fileBytes = "some text".getBytes(StandardCharsets.UTF_8);
		byte[] fileBytes2 = "some other longer text".getBytes(StandardCharsets.UTF_8);
		byte[] fileBytes3 = "even better text".getBytes(StandardCharsets.UTF_8);
		restClient.storeAssociated("index1", "123", "test.txt", null, fileBytes);
		restClient.storeAssociated("index1", "123", "test2.txt", new Document("aKey", "aValue"), fileBytes2);
		restClient.storeAssociated("index1", "456", "t.txt", null, fileBytes3);

		List<String> filenames;
		filenames = restClient.fetchAssociatedFilenamesForId("index1", "123");
		Assertions.assertEquals(2, filenames.size());
		Assertions.assertTrue(filenames.contains("test.txt"));
		Assertions.assertTrue(filenames.contains("test2.txt"));

		filenames = restClient.fetchAssociatedFilenamesForId("index1", "456");
		Assertions.assertEquals(1, filenames.size());
		Assertions.assertTrue(filenames.contains("t.txt"));

		{
			ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
			restClient.fetchAssociated("index1", "123", "test.txt", byteArrayOutputStream);
			byteArrayOutputStream.close();

			Assertions.assertArrayEquals(fileBytes, byteArrayOutputStream.toByteArray());
		}

		{
			ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
			restClient.fetchAssociated("index1", "123", "test2.txt", byteArrayOutputStream);
			byteArrayOutputStream.close();

			Assertions.assertArrayEquals(fileBytes2, byteArrayOutputStream.toByteArray());
		}

		Document metadata = restClient.fetchAssociatedMetadata("index1", "123", "test.txt");
		Assertions.assertEquals(0, metadata.size());

		Document metadata2 = restClient.fetchAssociatedMetadata("index1", "123", "test2.txt");
		Assertions.assertEquals(1, metadata2.size());
		Assertions.assertEquals("aValue", metadata2.getString("aKey"));

		ByteArrayOutputStream zipBytes = new ByteArrayOutputStream();
		restClient.fetchAssociatedBundle("index1", "123", zipBytes);

		boolean[] piecesFound = new boolean[5];
		try (ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(zipBytes.toByteArray()))) {
			ZipEntry ze;
			while ((ze = zis.getNextEntry()) != null) {
				var b = zis.readAllBytes();

				if (ze.getName().equals("test.txt/")) {
					piecesFound[0] = true;
					// dir
				}
				else if (ze.getName().equals("test2.txt/")) {
					piecesFound[1] = true;
					// dir
				}
				else if (ze.getName().equals("test.txt/test.txt")) {
					piecesFound[2] = true;
					Assertions.assertArrayEquals(fileBytes, b);
				}
				else if (ze.getName().equals("test2.txt/test2.txt")) {
					piecesFound[3] = true;
					Assertions.assertArrayEquals(fileBytes2, b);
				}
				else if (ze.getName().equals("test2.txt/test2.txt_metadata.json")) {
					piecesFound[4] = true;
					Assertions.assertEquals(Document.parse(new String(b)), new Document("aKey", "aValue"));
				}
				else {
					throw new Exception("Unexpected file <" + ze.getName());
				}
			}
		}

		for (boolean b : piecesFound) {
			Assertions.assertTrue(b);
		}

		List<AssociatedMetadataDTO> associatedMetadataDTOs = restClient.fetchAssociatedForIndex("index1");
		Assertions.assertEquals(3, associatedMetadataDTOs.size());

		associatedMetadataDTOs = restClient.fetchAssociatedForIndex("index1", new Document("filename", "t.txt"));
		Assertions.assertEquals(1, associatedMetadataDTOs.size());

		//table scan in mongo
		associatedMetadataDTOs = restClient.fetchAssociatedForIndex("index1", new Document("metadata.aKey", "aValue"));
		Assertions.assertEquals(1, associatedMetadataDTOs.size());
		Assertions.assertEquals("test2.txt", associatedMetadataDTOs.getFirst().filename());
		Assertions.assertEquals("123", associatedMetadataDTOs.getFirst().uniqueId());
		Assertions.assertEquals(1, associatedMetadataDTOs.getFirst().meta().size());
		Assertions.assertEquals("aValue", associatedMetadataDTOs.getFirst().meta().getString("aKey"));

	}

	@Test
	@Order(7)
	public void searchTest() throws Exception {

		ZuliaRESTClient restClient = restNodeExtension.getRESTClient();
		SearchResultsDTO searchResultsDTO;

		searchResultsDTO = restClient.search(new SearchREST("index1"));
		Assertions.assertEquals(3, searchResultsDTO.getTotalHits());

		searchResultsDTO = restClient.search(new SearchREST("index1"));
		Assertions.assertNull(searchResultsDTO.getResults());

		searchResultsDTO = restClient.search(new SearchREST("index1").setSort("title:1").setRows(1));
		Assertions.assertEquals("789", searchResultsDTO.getResults().getFirst().getId());

		searchResultsDTO = restClient.search(new SearchREST("index1").setSort("title:" + ZuliaRESTConstants.ASC).setRows(1));
		Assertions.assertEquals("789", searchResultsDTO.getResults().getFirst().getId());

		searchResultsDTO = restClient.search(new SearchREST("index1").setSort("title:-1").setRows(1));
		Assertions.assertEquals("123", searchResultsDTO.getResults().getFirst().getId());

		searchResultsDTO = restClient.search(new SearchREST("index1").setSort("title:" + ZuliaRESTConstants.DESC).setRows(1));
		Assertions.assertEquals("123", searchResultsDTO.getResults().getFirst().getId());

		searchResultsDTO = restClient.search(new SearchREST("index1").setRows(1));
		Assertions.assertEquals(1, searchResultsDTO.getResults().size());
		Assertions.assertEquals(1.0, searchResultsDTO.getResults().getFirst().getScore(), 0.001);

		searchResultsDTO = restClient.search(new SearchREST("index1").setQuery("*:*"));
		Assertions.assertEquals(3, searchResultsDTO.getTotalHits());

		{
			searchResultsDTO = restClient.search(new SearchREST("index1").setQuery("title:value").setRows(1));
			Assertions.assertEquals(1, searchResultsDTO.getTotalHits());
			Document doc = searchResultsDTO.getResults().getFirst().getDocument();
			Assertions.assertEquals("some value", doc.getString("title"));
			Assertions.assertEquals("the best value", doc.getString("notIndexed"));
		}

		{
			searchResultsDTO = restClient.search(new SearchREST("index1").setQuery("title:value").setFields("title", "notIndexed").setRows(1));
			Assertions.assertEquals(1, searchResultsDTO.getTotalHits());
			Document doc = searchResultsDTO.getResults().getFirst().getDocument();
			Assertions.assertEquals("some value", doc.getString("title"));
			Assertions.assertEquals("the best value", doc.getString("notIndexed"));
		}

		{
			searchResultsDTO = restClient.search(new SearchREST("index1").setQuery("value").setQueryFields("title").setRows(1));
			Assertions.assertEquals(1, searchResultsDTO.getTotalHits());
			Document doc = searchResultsDTO.getResults().getFirst().getDocument();
			Assertions.assertEquals("some value", doc.getString("title"));
			Assertions.assertEquals("the best value", doc.getString("notIndexed"));
		}

		{
			searchResultsDTO = restClient.search(new SearchREST("index1").setFilterQueries("title:some", "title:value").setRows(1));
			Assertions.assertEquals(1, searchResultsDTO.getTotalHits());
			Document doc = searchResultsDTO.getResults().getFirst().getDocument();
			Assertions.assertEquals("some value", doc.getString("title"));
			Assertions.assertEquals("the best value", doc.getString("notIndexed"));
		}

		{
			searchResultsDTO = restClient.search(new SearchREST("index1").setFilterQueries("title:some", "title:other").setRows(1));
			Assertions.assertEquals(0, searchResultsDTO.getTotalHits());
		}

		{
			searchResultsDTO = restClient.search(new SearchREST("index1").setQuery("title:value").setFields("title").setRows(1));
			Assertions.assertEquals(1, searchResultsDTO.getTotalHits());
			Document doc = searchResultsDTO.getResults().getFirst().getDocument();
			Assertions.assertEquals("some value", doc.getString("title"));
			Assertions.assertNull(doc.getString("notIndexed"));
		}

		{
			searchResultsDTO = restClient.search(new SearchREST("index1").setFacet("year"));
			Assertions.assertEquals(3, searchResultsDTO.getTotalHits());
			FacetsDTO firstFacetField = searchResultsDTO.getFacets().getFirst();
			FacetDTO topFacet = firstFacetField.values().getFirst();
			Assertions.assertEquals("2022", topFacet.facet());
			Assertions.assertEquals(2L, topFacet.count());
		}

		{
			searchResultsDTO = restClient.search(new SearchREST("index1").setFacet("year").setDrillDowns("year:2021"));
			Assertions.assertEquals(1, searchResultsDTO.getTotalHits());
			FacetsDTO firstFacetField = searchResultsDTO.getFacets().getFirst();
			FacetDTO topFacet = firstFacetField.values().getFirst();
			Assertions.assertEquals("2021", topFacet.facet());
			Assertions.assertEquals(1L, topFacet.count());
		}

		{
			searchResultsDTO = restClient.search(new SearchREST("index1").setQuery("title:value").setHighlights("title").setRows(1));
			Assertions.assertEquals(1, searchResultsDTO.getTotalHits());
			ScoredResultDTO scoredResultDTO = searchResultsDTO.getResults().getFirst();
			HighlightDTO highlightDTO = scoredResultDTO.getHighlights().getFirst();
			Assertions.assertEquals("title", highlightDTO.field());
			Assertions.assertEquals("some <em>value</em>", highlightDTO.fragments().getFirst());
		}

		searchResultsDTO = restClient.search(new SearchREST("index1").setQuery("title:madeupthings"));
		Assertions.assertEquals(0, searchResultsDTO.getTotalHits());

	}

}
