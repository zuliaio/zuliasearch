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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

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

	@Test
	public void deletesTolerateMissingFilesAndDirectories() {
		FileDocumentStorage storage = new FileDocumentStorage(dataPath.toString(), "testIndex");

		Assertions.assertDoesNotThrow(() -> storage.deleteAssociatedDocuments("neverStored"));
		Assertions.assertDoesNotThrow(() -> storage.deleteAssociatedDocument("neverStored", "missing.txt"));
		Assertions.assertDoesNotThrow(storage::deleteAllDocuments);
		Assertions.assertDoesNotThrow(storage::drop);
	}

	@Test
	public void shortUniqueIdsStoreFetchAndDelete() throws Exception {
		FileDocumentStorage storage = new FileDocumentStorage(dataPath.toString(), "testIndex");

		for (String uniqueId : List.of("a1", "a3", "z")) {
			byte[] content = ("content for " + uniqueId).getBytes(StandardCharsets.UTF_8);
			AssociatedDocument associatedDocument = AssociatedDocument.newBuilder().setDocumentUniqueId(uniqueId).setFilename("data.txt")
					.setIndexName("testIndex").setDocument(ByteString.copyFrom(content)).setTimestamp(1L).build();

			storage.storeAssociatedDocument(associatedDocument);

			Assertions.assertEquals(List.of("data.txt"), storage.getAssociatedFilenames(uniqueId));

			AssociatedDocument fetched = storage.getAssociatedDocument(uniqueId, "data.txt", FetchType.FULL);
			Assertions.assertArrayEquals(content, fetched.getDocument().toByteArray());

			storage.deleteAssociatedDocuments(uniqueId);
			Assertions.assertTrue(storage.getAssociatedFilenames(uniqueId).isEmpty());
		}
	}

	@Test
	public void getAssociatedFilenamesReturnsOnlyDocuments() throws Exception {
		FileDocumentStorage storage = new FileDocumentStorage(dataPath.toString(), "testIndex");

		storage.storeAssociatedDocument(buildDocument("docList", "notes.txt"));
		storage.storeAssociatedDocument(buildDocument("docList", "summary.txt"));

		Assertions.assertEquals(List.of("notes.txt", "summary.txt"), storage.getAssociatedFilenames("docList"));

		storage.storeAssociatedDocument(buildDocument("docList", "report.metadata"));

		Assertions.assertEquals(List.of("notes.txt", "report.metadata", "summary.txt"), storage.getAssociatedFilenames("docList"));
	}

	@Test
	public void deleteAssociatedDocumentRemovesMetadataSidecar() throws Exception {
		FileDocumentStorage storage = new FileDocumentStorage(dataPath.toString(), "testIndex");

		storage.storeAssociatedDocument(buildDocument("docDel", "notes.txt"));
		storage.storeAssociatedDocument(buildDocument("docDel", "summary.txt"));

		storage.deleteAssociatedDocument("docDel", "notes.txt");

		Assertions.assertEquals(List.of("summary.txt"), storage.getAssociatedFilenames("docDel"));

		try (Stream<Path> walk = Files.walk(dataPath)) {
			Assertions.assertTrue(walk.map(Path::getFileName).map(Path::toString).noneMatch(name -> name.startsWith("notes.txt")),
					"data file and metadata sidecar should both be deleted");
		}
	}

	@Test
	public void traversalFilenameIsRejectedOnEveryEntryPoint() throws Exception {
		FileDocumentStorage storage = new FileDocumentStorage(dataPath.toString(), "testIndex");
		String escaping = "../../../../../../escaped.txt";

		IllegalArgumentException stored = Assertions.assertThrows(IllegalArgumentException.class,
				() -> storage.storeAssociatedDocument(buildDocument("docTraversal", escaping)));
		Assertions.assertTrue(stored.getMessage().contains(escaping) && stored.getMessage().contains("testIndex"), stored.getMessage());

		Assertions.assertThrows(IllegalArgumentException.class,
				() -> storage.getAssociatedDocumentOutputStream("docTraversal", escaping, 1L, new Document()));
		Assertions.assertThrows(IllegalArgumentException.class, () -> storage.getAssociatedDocument("docTraversal", escaping, FetchType.FULL));
		Assertions.assertThrows(IllegalArgumentException.class, () -> storage.getAssociatedDocumentStream("docTraversal", escaping));
		Assertions.assertThrows(IllegalArgumentException.class, () -> storage.deleteAssociatedDocument("docTraversal", escaping));

		try (Stream<Path> walk = Files.walk(dataPath)) {
			Assertions.assertTrue(walk.map(Path::getFileName).map(Path::toString).noneMatch(name -> name.startsWith("escaped.txt")),
					"nothing may be written for a rejected filename");
		}
	}

	@Test
	public void traversalUniqueIdIsRejectedOnEveryEntryPoint() {
		FileDocumentStorage storage = new FileDocumentStorage(dataPath.toString(), "testIndex");
		String escaping = "../../../../../../escapedDir";

		IllegalArgumentException stored = Assertions.assertThrows(IllegalArgumentException.class,
				() -> storage.storeAssociatedDocument(buildDocument(escaping, "notes.txt")));
		Assertions.assertTrue(stored.getMessage().contains(escaping), stored.getMessage());

		Assertions.assertThrows(IllegalArgumentException.class, () -> storage.getAssociatedDocument(escaping, "notes.txt", FetchType.META));
		Assertions.assertThrows(IllegalArgumentException.class, () -> storage.getAssociatedFilenames(escaping));
		Assertions.assertThrows(IllegalArgumentException.class, () -> storage.deleteAssociatedDocument(escaping, "notes.txt"));
		Assertions.assertThrows(IllegalArgumentException.class, () -> storage.deleteAssociatedDocuments(escaping));
		Assertions.assertFalse(Files.exists(dataPath.resolve("escapedDir")));
	}

	@Test
	public void absoluteAndInvalidFilenamesAreRejected() {
		FileDocumentStorage storage = new FileDocumentStorage(dataPath.toString(), "testIndex");

		Assertions.assertThrows(IllegalArgumentException.class,
				() -> storage.storeAssociatedDocument(buildDocument("docAbs", dataPath.resolve("outside.txt").toString())));

		IllegalArgumentException nul = Assertions.assertThrows(IllegalArgumentException.class,
				() -> storage.storeAssociatedDocument(buildDocument("docNul", "notes\u0000.txt")));
		Assertions.assertTrue(nul.getMessage().contains("not a valid path"), nul.getMessage());

		IllegalArgumentException blank = Assertions.assertThrows(IllegalArgumentException.class,
				() -> storage.storeAssociatedDocument(buildDocument("docBlank", " ")));
		Assertions.assertTrue(blank.getMessage().contains("must not be blank"), blank.getMessage());
	}

	@Test
	public void uniqueIdWithSeparatorStaysInsideTheIndexRoot() throws Exception {
		FileDocumentStorage storage = new FileDocumentStorage(dataPath.toString(), "testIndex");
		String doi = "10.1000/journal.2024.17";

		storage.storeAssociatedDocument(buildDocument(doi, "paper.pdf"));

		Assertions.assertEquals(List.of("paper.pdf"), storage.getAssociatedFilenames(doi));
		Assertions.assertArrayEquals("paper.pdf".getBytes(StandardCharsets.UTF_8),
				storage.getAssociatedDocument(doi, "paper.pdf", FetchType.FULL).getDocument().toByteArray());
		try (Stream<Path> walk = Files.walk(dataPath.resolve("files").resolve("testIndex"))) {
			Assertions.assertTrue(walk.anyMatch(path -> path.getFileName().toString().equals("paper.pdf")));
		}

		storage.deleteAssociatedDocuments(doi);
		Assertions.assertEquals(List.of(), storage.getAssociatedFilenames(doi));
	}

	private static AssociatedDocument buildDocument(String uniqueId, String filename) {
		return AssociatedDocument.newBuilder().setDocumentUniqueId(uniqueId).setFilename(filename).setIndexName("testIndex")
				.setDocument(ByteString.copyFrom(filename.getBytes(StandardCharsets.UTF_8))).setTimestamp(1L).build();
	}
}
