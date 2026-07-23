package io.zulia.server.test.util;

import com.google.protobuf.ByteString;
import com.mongodb.client.MongoClient;
import io.zulia.message.ZuliaBase.AssociatedDocument;
import io.zulia.message.ZuliaQuery.FetchType;
import io.zulia.server.filestorage.DocumentStorage;
import io.zulia.server.filestorage.MongoDocumentStorage;
import io.zulia.server.util.MongoProvider;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

public class MongoDocumentStorageTest {

	private static final String DB_NAME = "mongoDocumentStorageTestFs";
	private static final String FILES_COLLECTION = "associatedFiles.files";

	@AfterEach
	public void cleanup() {
		MongoProvider.getMongoClient().getDatabase(DB_NAME).drop();
	}

	@Test
	public void databaseIsOnlyCreatedOnFirstStore() throws Exception {
		MongoClient client = MongoProvider.getMongoClient();
		MongoDocumentStorage storage = new MongoDocumentStorage(client, "lazyIndex", DB_NAME, false);

		Assertions.assertNull(storage.getAssociatedDocument("id1", "notes.txt", FetchType.FULL));
		Assertions.assertTrue(storage.getAssociatedMetadataForUniqueId("id1", FetchType.FULL).isEmpty());
		Assertions.assertTrue(storage.getAssociatedFilenames("id1").isEmpty());
		storage.deleteAssociatedDocument("id1", "notes.txt");
		storage.deleteAssociatedDocuments("id1");
		storage.deleteAllDocuments();

		Assertions.assertFalse(databaseExists(client), "reads and deletes must not create the associated files database");

		storage.storeAssociatedDocument(buildDocument("id1", "notes.txt"));

		Assertions.assertTrue(databaseExists(client), "storing a file should create the associated files database");
		Assertions.assertTrue(hasMetadataIndexes(client), "storing a file should create the metadata indexes");

		AssociatedDocument fetched = storage.getAssociatedDocument("id1", "notes.txt", FetchType.FULL);
		Assertions.assertArrayEquals("notes.txt".getBytes(StandardCharsets.UTF_8), fetched.getDocument().toByteArray());
	}

	@Test
	public void metadataIndexesAreRecreatedAfterDeleteAll() throws Exception {
		MongoClient client = MongoProvider.getMongoClient();
		MongoDocumentStorage storage = new MongoDocumentStorage(client, "lazyIndex", DB_NAME, false);

		storage.storeAssociatedDocument(buildDocument("id1", "notes.txt"));
		Assertions.assertTrue(hasMetadataIndexes(client));

		storage.deleteAllDocuments();
		Assertions.assertTrue(storage.getAssociatedFilenames("id1").isEmpty());

		storage.storeAssociatedDocument(buildDocument("id2", "summary.txt"));

		Assertions.assertTrue(hasMetadataIndexes(client), "metadata indexes should be recreated after a clear dropped them");
		Assertions.assertEquals(1, storage.getAssociatedMetadataForUniqueId("id2", FetchType.META).size());
	}

	private static boolean databaseExists(MongoClient client) {
		for (String name : client.listDatabaseNames()) {
			if (DB_NAME.equals(name)) {
				return true;
			}
		}
		return false;
	}

	private static boolean hasMetadataIndexes(MongoClient client) {
		Set<String> indexedFields = new HashSet<>();
		for (Document index : client.getDatabase(DB_NAME).getCollection(FILES_COLLECTION).listIndexes()) {
			indexedFields.addAll(index.get("key", Document.class).keySet());
		}
		return indexedFields.contains("metadata." + DocumentStorage.DOCUMENT_UNIQUE_ID_KEY) && indexedFields.contains(
				"metadata." + DocumentStorage.FILE_UNIQUE_ID_KEY);
	}

	private static AssociatedDocument buildDocument(String uniqueId, String filename) {
		return AssociatedDocument.newBuilder().setDocumentUniqueId(uniqueId).setFilename(filename).setIndexName("lazyIndex")
				.setDocument(ByteString.copyFrom(filename.getBytes(StandardCharsets.UTF_8))).setTimestamp(1L).build();
	}
}
