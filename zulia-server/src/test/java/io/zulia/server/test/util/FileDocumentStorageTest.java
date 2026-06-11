package io.zulia.server.test.util;

import com.google.protobuf.ByteString;
import io.zulia.message.ZuliaBase.AssociatedDocument;
import io.zulia.message.ZuliaQuery.FetchType;
import io.zulia.server.filestorage.FileDocumentStorage;
import io.zulia.util.ZuliaUtil;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

public class FileDocumentStorageTest {

	@TempDir
	Path dataPath;

	@Test
	public void storeAndFetchAssociatedDocumentWithMetadata() throws Exception {
		FileDocumentStorage storage = new FileDocumentStorage(dataPath.toString(), "testIndex");

		Document metadata = new Document();
		metadata.put("contentType", "text/plain");
		metadata.put("source", "unitTest");

		byte[] content = "hello associated".getBytes(StandardCharsets.UTF_8);

		AssociatedDocument associatedDocument = AssociatedDocument.newBuilder().setDocumentUniqueId("doc1").setFilename("notes.txt")
				.setIndexName("testIndex").setDocument(ByteString.copyFrom(content)).setMetadata(ZuliaUtil.mongoDocumentToByteString(metadata))
				.setTimestamp(12345L).build();

		storage.storeAssociatedDocument(associatedDocument);

		AssociatedDocument fetched = storage.getAssociatedDocument("doc1", "notes.txt", FetchType.FULL);

		Assertions.assertEquals("doc1", fetched.getDocumentUniqueId());
		Assertions.assertEquals("notes.txt", fetched.getFilename());
		Assertions.assertEquals(12345L, fetched.getTimestamp());
		Assertions.assertArrayEquals(content, fetched.getDocument().toByteArray());

		Document fetchedMetadata = ZuliaUtil.byteStringToMongoDocument(fetched.getMetadata());
		Assertions.assertEquals("text/plain", fetchedMetadata.getString("contentType"));
		Assertions.assertEquals("unitTest", fetchedMetadata.getString("source"));
	}

	@Test
	public void storeAndFetchAssociatedDocumentWithoutMetadata() throws Exception {
		FileDocumentStorage storage = new FileDocumentStorage(dataPath.toString(), "testIndex");

		AssociatedDocument associatedDocument = AssociatedDocument.newBuilder().setDocumentUniqueId("doc2").setFilename("empty.txt")
				.setIndexName("testIndex").setDocument(ByteString.copyFrom("x".getBytes(StandardCharsets.UTF_8))).setTimestamp(67890L).build();

		storage.storeAssociatedDocument(associatedDocument);

		AssociatedDocument fetched = storage.getAssociatedDocument("doc2", "empty.txt", FetchType.FULL);

		Assertions.assertEquals(67890L, fetched.getTimestamp());
		Assertions.assertTrue(ZuliaUtil.byteStringToMongoDocument(fetched.getMetadata()).isEmpty());
	}
}
