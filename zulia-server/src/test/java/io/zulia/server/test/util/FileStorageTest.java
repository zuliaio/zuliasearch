package io.zulia.server.test.util;

import io.zulia.DefaultAnalyzers;
import io.zulia.client.command.CreateIndex;
import io.zulia.client.command.DeleteAssociated;
import io.zulia.client.command.Query;
import io.zulia.client.command.Store;
import io.zulia.client.command.StoreLargeAssociated;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.config.ZuliaPoolConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.QueryResult;
import io.zulia.doc.AssociatedBuilder;
import io.zulia.doc.ResultDocBuilder;
import io.zulia.fields.FieldConfigBuilder;
import io.zulia.message.ZuliaIndex;
import org.bson.Document;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class FileStorageTest {

	public static final String TEST_INDEX = "testIndex";
	private ZuliaWorkPool zuliaWorkPool;

	public static void main(String[] args) throws Exception {

		FileStorageTest fileStorageTest = new FileStorageTest();
		System.out.println("Initiating work pool.");
		fileStorageTest.init();
		System.out.println("Creating index.");
		fileStorageTest.createIndex();

		// This needs to be changed to a local dir with PDF files.
		String pdfsDir = "/data/chemrxiv";

		System.out.println("Indexing documents.");
		AtomicInteger idCounter = new AtomicInteger(1);
		Files.list(Paths.get(pdfsDir)).forEach(file -> {
			if (file.toFile().getName().endsWith("pdf")) {
				Document doc = new Document("id", idCounter.getAndIncrement() + "");
				doc.put("title", fileStorageTest.getRandomString());
				doc.put("abstract", fileStorageTest.getRandomString());

				try {
					fileStorageTest.indexDocument(doc);
					Document meta = new Document();
					meta.put("extension", "pdf");
					meta.put("contentType", "application/pdf");
					fileStorageTest.storeFile(doc.getString("id"), file.toFile().getName(), meta, Files.readAllBytes(file));
				}
				catch (Exception e) {
					e.printStackTrace();
				}

				if (idCounter.get() % 100 == 0) {
					System.out.println("So far indexed: " + idCounter);
				}

			}
		});
		System.out.println("Finished indexing: " + idCounter);

		fileStorageTest.query();

		System.out.println("All good, clean up.");
		fileStorageTest.cleanUp();

	}

	private void cleanUp() throws Exception {
		zuliaWorkPool.deleteIndex(TEST_INDEX);
		zuliaWorkPool.shutdown();
	}

	private void query() throws Exception {
		Query query = new Query(TEST_INDEX, "*:*", 10000);
		QueryResult queryResult = zuliaWorkPool.query(query);
		System.out.println("Results: " + queryResult.getTotalHits());
	}

	private void init() throws Exception {
		ZuliaPoolConfig zuliaPoolConfig = new ZuliaPoolConfig();
		zuliaPoolConfig.addNode("localhost", 32191, 32192);

		zuliaPoolConfig.setDefaultRetries(1);
		zuliaPoolConfig.setNodeUpdateEnabled(false);
		zuliaPoolConfig.setMaxConnections(32);

		zuliaWorkPool = new ZuliaWorkPool(zuliaPoolConfig);
	}

	private void createIndex() throws Exception {
		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.setIndexName(TEST_INDEX);
		indexConfig.setNumberOfShards(1);
		indexConfig.setShardCommitInterval(64000);

		indexConfig.addAnalyzerSetting(
				ZuliaIndex.AnalyzerSettings.newBuilder().setName("text").addFilter(ZuliaIndex.AnalyzerSettings.Filter.CASE_PROTECTED_WORDS)
						.addFilter(ZuliaIndex.AnalyzerSettings.Filter.LOWERCASE).addFilter(ZuliaIndex.AnalyzerSettings.Filter.ASCII_FOLDING)
						.addFilter(ZuliaIndex.AnalyzerSettings.Filter.ENGLISH_POSSESSIVE).addFilter(ZuliaIndex.AnalyzerSettings.Filter.ENGLISH_MIN_STEM)
						.addFilter(ZuliaIndex.AnalyzerSettings.Filter.BRITISH_US).build());
		indexConfig.addAnalyzerSetting(ZuliaIndex.AnalyzerSettings.newBuilder().setName("entity").addFilter(ZuliaIndex.AnalyzerSettings.Filter.LOWERCASE)
				.addFilter(ZuliaIndex.AnalyzerSettings.Filter.ASCII_FOLDING).build());

		FieldConfigBuilder fieldConfigBuilder = FieldConfigBuilder.create("title", ZuliaIndex.FieldConfig.FieldType.STRING);
		fieldConfigBuilder.indexAs("text", "title");
		fieldConfigBuilder.displayName("Title");
		indexConfig.addFieldConfig(fieldConfigBuilder);

		FieldConfigBuilder fieldConfigBuilder2 = FieldConfigBuilder.create("abstract", ZuliaIndex.FieldConfig.FieldType.STRING);
		fieldConfigBuilder2.indexAs("text", "abstract");
		fieldConfigBuilder2.displayName("Abstract");
		indexConfig.addFieldConfig(fieldConfigBuilder2);

		FieldConfigBuilder fieldConfigBuilder3 = FieldConfigBuilder.create("id", ZuliaIndex.FieldConfig.FieldType.STRING);
		fieldConfigBuilder3.indexAs(DefaultAnalyzers.STANDARD, "id");
		fieldConfigBuilder3.displayName("ID");
		indexConfig.addFieldConfig(fieldConfigBuilder3);

		CreateIndex createOrUpdateIndex = new CreateIndex(indexConfig);
		zuliaWorkPool.createIndex(createOrUpdateIndex);
	}

	private void storeFile(String documentId, String filename, Document meta, byte[] content) throws Exception {

		zuliaWorkPool.delete(new DeleteAssociated(documentId, TEST_INDEX, filename));

		if (content.length > 32 * 1024 * 1024) {
			StoreLargeAssociated storeLargeAssociated = new StoreLargeAssociated(documentId, TEST_INDEX, filename, content);
			storeLargeAssociated.setMeta(meta);
			zuliaWorkPool.storeLargeAssociated(storeLargeAssociated);
		}
		else {
			Store associatedDocStore = new Store(documentId, TEST_INDEX);
			associatedDocStore.addAssociatedDocument(AssociatedBuilder.newBuilder().setDocument(content).setFilename(filename).setMetadata(meta));
			zuliaWorkPool.store(associatedDocStore);
		}

	}

	private void indexDocument(Document document) throws Exception {
		Store store = new Store(document.getString("id"), TEST_INDEX);
		store.setResultDocument(new ResultDocBuilder().setDocument(document));
		zuliaWorkPool.store(store);
	}

	private String getRandomString() {
		int leftLimit = 97; // letter 'a'
		int rightLimit = 122; // letter 'z'
		int targetStringLength = 10;
		Random random = new Random();

		String generatedString = random.ints(leftLimit, rightLimit + 1).limit(targetStringLength)
				.collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append).toString();

		return generatedString;
	}

}
