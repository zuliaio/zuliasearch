package io.zulia.server.test.node;

import io.zulia.DefaultAnalyzers;
import io.zulia.client.command.Store;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.rest.ZuliaRESTClient;
import io.zulia.client.rest.options.TermsRestOptions;
import io.zulia.fields.FieldConfigBuilder;
import io.zulia.message.ZuliaServiceOuterClass.RestIndexSettingsResponse;
import io.zulia.rest.dto.FieldsDTO;
import io.zulia.rest.dto.IndexMappingDTO;
import io.zulia.rest.dto.IndexesResponseDTO;
import io.zulia.rest.dto.NodeDTO;
import io.zulia.rest.dto.NodesResponseDTO;
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
			indexConfig.addFieldConfig(FieldConfigBuilder.createString("title").indexAs(DefaultAnalyzers.STANDARD));
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
			store.setResultDocument(new Document("id", "123").append("title", "test").append("notIndexed", "some value"));
			zuliaWorkPool.store(store);
		}

		{
			Store store = new Store("456", "index1");
			store.setResultDocument(new Document("id", "456").append("title", "some value").append("notIndexed", "the best value"));
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
		Assertions.assertEquals(2, restIndexSettingsResponse.getIndexSettings().getFieldConfigList().size());
		Assertions.assertEquals("id", restIndexSettingsResponse.getIndexSettings().getFieldConfigList().get(0).getStoredFieldName());
		Assertions.assertEquals("title", restIndexSettingsResponse.getIndexSettings().getFieldConfigList().get(1).getStoredFieldName());
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
		Assertions.assertEquals(3, termsResponseDTO.getTerms().size());
		Assertions.assertEquals("some", termsResponseDTO.getTerms().get(0).getTerm());
		Assertions.assertEquals("test", termsResponseDTO.getTerms().get(1).getTerm());
		Assertions.assertEquals("value", termsResponseDTO.getTerms().get(2).getTerm());

		termsResponseDTO = restClient.getTerms("index1", "title", new TermsRestOptions().setAmount(1));
		Assertions.assertEquals(1, termsResponseDTO.getTerms().size());
		Assertions.assertEquals("some", termsResponseDTO.getTerms().get(0).getTerm());

		termsResponseDTO = restClient.getTerms("index1", "title", new TermsRestOptions().setAmount(1).setStartTerm("t"));
		Assertions.assertEquals(1, termsResponseDTO.getTerms().size());
		Assertions.assertEquals("test", termsResponseDTO.getTerms().get(0).getTerm());

		termsResponseDTO = restClient.getTerms("index1", "title", new TermsRestOptions().setTermFilter("test"));
		Assertions.assertEquals(2, termsResponseDTO.getTerms().size());
		Assertions.assertEquals("some", termsResponseDTO.getTerms().getFirst().getTerm());
		Assertions.assertEquals("value", termsResponseDTO.getTerms().getLast().getTerm());

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
		filenames = restClient.fetchAssociatedFilenames("index1", "123");
		Assertions.assertEquals(2, filenames.size());
		Assertions.assertTrue(filenames.contains("test.txt"));
		Assertions.assertTrue(filenames.contains("test2.txt"));

		filenames = restClient.fetchAssociatedFilenames("index1", "456");
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

	}

}
