package io.zulia.server.test.util;

import com.google.protobuf.ByteString;
import io.zulia.message.ZuliaBase.AssociatedDocument;
import io.zulia.message.ZuliaQuery.FetchType;
import io.zulia.server.filestorage.FileDocumentStorage;
import io.zulia.server.index.ZuliaIndexVersion;
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
		FileDocumentStorage storage = new FileDocumentStorage(dataPath.toString(), "testIndex", ZuliaIndexVersion.CURRENT);

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
		FileDocumentStorage storage = new FileDocumentStorage(dataPath.toString(), "testIndex", ZuliaIndexVersion.CURRENT);

		AssociatedDocument associatedDocument = AssociatedDocument.newBuilder().setDocumentUniqueId("doc2").setFilename("empty.txt")
				.setIndexName("testIndex").setDocument(ByteString.copyFrom("x".getBytes(StandardCharsets.UTF_8))).setTimestamp(67890L).build();

		storage.storeAssociatedDocument(associatedDocument);

		AssociatedDocument fetched = storage.getAssociatedDocument("doc2", "empty.txt", FetchType.FULL);

		Assertions.assertEquals(67890L, fetched.getTimestamp());
		Assertions.assertTrue(ZuliaUtil.byteStringToMongoDocument(fetched.getMetadata()).isEmpty());
	}

	@Test
	public void deletesTolerateMissingFilesAndDirectories() {
		FileDocumentStorage storage = new FileDocumentStorage(dataPath.toString(), "testIndex", ZuliaIndexVersion.CURRENT);

		Assertions.assertDoesNotThrow(() -> storage.deleteAssociatedDocuments("neverStored"));
		Assertions.assertDoesNotThrow(() -> storage.deleteAssociatedDocument("neverStored", "missing.txt"));
		Assertions.assertDoesNotThrow(storage::deleteAllDocuments);
		Assertions.assertDoesNotThrow(storage::drop);
	}

	@Test
	public void shortUniqueIdsStoreFetchAndDelete() throws Exception {
		FileDocumentStorage storage = new FileDocumentStorage(dataPath.toString(), "testIndex", ZuliaIndexVersion.CURRENT);

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
		FileDocumentStorage storage = new FileDocumentStorage(dataPath.toString(), "testIndex", ZuliaIndexVersion.CURRENT);

		storage.storeAssociatedDocument(buildDocument("docList", "notes.txt"));
		storage.storeAssociatedDocument(buildDocument("docList", "summary.txt"));

		Assertions.assertEquals(List.of("notes.txt", "summary.txt"), storage.getAssociatedFilenames("docList"));

		storage.storeAssociatedDocument(buildDocument("docList", "report.metadata"));

		Assertions.assertEquals(List.of("notes.txt", "report.metadata", "summary.txt"), storage.getAssociatedFilenames("docList"));
	}

	@Test
	public void deleteAssociatedDocumentRemovesMetadataSidecar() throws Exception {
		FileDocumentStorage storage = new FileDocumentStorage(dataPath.toString(), "testIndex", ZuliaIndexVersion.CURRENT);

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
		FileDocumentStorage storage = new FileDocumentStorage(dataPath.toString(), "testIndex", ZuliaIndexVersion.CURRENT);
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
		FileDocumentStorage storage = new FileDocumentStorage(dataPath.toString(), "testIndex", ZuliaIndexVersion.CURRENT);
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
		FileDocumentStorage storage = new FileDocumentStorage(dataPath.toString(), "testIndex", ZuliaIndexVersion.CURRENT);

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
		FileDocumentStorage storage = new FileDocumentStorage(dataPath.toString(), "testIndex", ZuliaIndexVersion.CURRENT);
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

	@Test
	public void uniqueIdCannotEscapeItsOwnHashBucket() throws Exception {
		FileDocumentStorage storage = new FileDocumentStorage(dataPath.toString(), "testIndex", ZuliaIndexVersion.CURRENT);
		// both ids hash into bucket 1e/bd6, so an id that climbs to the bucket would reach the victim's files
		String victim = "doc312082300";
		String climbing = "x/../..";

		storage.storeAssociatedDocument(buildDocument(victim, "notes.txt"));

		Assertions.assertThrows(IllegalArgumentException.class, () -> storage.storeAssociatedDocument(buildDocument(climbing, "evil.txt")));
		Assertions.assertThrows(IllegalArgumentException.class, () -> storage.deleteAssociatedDocuments(climbing));
		Assertions.assertThrows(IllegalArgumentException.class, () -> storage.deleteAssociatedDocuments("../.."));
		Assertions.assertThrows(IllegalArgumentException.class, () -> storage.getAssociatedFilenames("x/.."));

		Assertions.assertEquals(List.of("notes.txt"), storage.getAssociatedFilenames(victim));
	}

	@Test
	public void climbingIdWithCollidingHashCannotReachAnotherIdsDirectory() throws Exception {
		FileDocumentStorage storage = new FileDocumentStorage(dataPath.toString(), "testIndex", ZuliaIndexVersion.CURRENT);
		String victim = "doc-1";
		// "aanpzrpv/../" hashes to zero, so this id shares the victim's bucket and normalizes to the victim's directory
		String climbing = "aanpzrpv/../" + victim;
		Assertions.assertEquals(victim.hashCode(), climbing.hashCode(), "test precondition: the ids must share a hash bucket");

		storage.storeAssociatedDocument(buildDocument(victim, "notes.txt"));

		IllegalArgumentException stored = Assertions.assertThrows(IllegalArgumentException.class,
				() -> storage.storeAssociatedDocument(buildDocument(climbing, "evil.txt")));
		Assertions.assertTrue(stored.getMessage().contains("relative path segment"), stored.getMessage());
		Assertions.assertThrows(IllegalArgumentException.class, () -> storage.deleteAssociatedDocuments(climbing));
		Assertions.assertThrows(IllegalArgumentException.class, () -> storage.getAssociatedFilenames("./" + victim));

		Assertions.assertEquals(List.of("notes.txt"), storage.getAssociatedFilenames(victim));
		Assertions.assertArrayEquals("notes.txt".getBytes(StandardCharsets.UTF_8),
				storage.getAssociatedDocument(victim, "notes.txt", FetchType.FULL).getDocument().toByteArray());
	}

	@Test
	public void metadataSidecarsLiveUnderMetaDirectoryAndAnyFilenameIsAllowed() throws Exception {
		FileDocumentStorage storage = new FileDocumentStorage(dataPath.toString(), "testIndex", ZuliaIndexVersion.CURRENT);
		Assertions.assertEquals(FileDocumentStorage.FileLayout.DJB2_META_DIR, storage.getFileLayout());

		storage.storeAssociatedDocument(buildDocument("docMeta", "run"));
		storage.storeAssociatedDocument(buildDocument("docMeta", "run.metadata"));

		Assertions.assertEquals(List.of("run", "run.metadata"), storage.getAssociatedFilenames("docMeta"));
		Assertions.assertArrayEquals("run".getBytes(StandardCharsets.UTF_8), storage.getAssociatedDocument("docMeta", "run", FetchType.FULL).getDocument().toByteArray());
		Assertions.assertArrayEquals("run.metadata".getBytes(StandardCharsets.UTF_8),
				storage.getAssociatedDocument("docMeta", "run.metadata", FetchType.FULL).getDocument().toByteArray());
		try (Stream<Path> walk = Files.walk(dataPath)) {
			List<String> sidecars = walk.filter(p -> p.getParent() != null && p.getParent().getFileName().toString().equals(".meta")).map(p -> p.getFileName().toString())
					.sorted().toList();
			Assertions.assertEquals(List.of("run", "run.metadata"), sidecars, "one sidecar per document, both under .meta");
		}

		storage.deleteAssociatedDocument("docMeta", "run");
		Assertions.assertEquals(List.of("run.metadata"), storage.getAssociatedFilenames("docMeta"));
	}

	@Test
	public void legacyLayoutKeepsSidecarsBesideFilesAndReservesTheSuffix() throws Exception {
		FileDocumentStorage storage = new FileDocumentStorage(dataPath.toString(), "testIndex", ZuliaIndexVersion.ASSOCIATED_FILE_LAYOUT_V2 - 1);
		Assertions.assertEquals(FileDocumentStorage.FileLayout.LEGACY, storage.getFileLayout());

		storage.storeAssociatedDocument(buildDocument("docLegacy", "notes.txt"));
		try (Stream<Path> walk = Files.walk(dataPath)) {
			Assertions.assertTrue(walk.anyMatch(p -> p.getFileName().toString().equals("notes.txt.metadata")), "legacy sidecar sits beside the file");
		}
		Assertions.assertEquals(List.of("notes.txt"), storage.getAssociatedFilenames("docLegacy"));

		IllegalArgumentException rejected = Assertions.assertThrows(IllegalArgumentException.class,
				() -> storage.storeAssociatedDocument(buildDocument("docLegacy", "notes.txt.metadata")));
		Assertions.assertTrue(rejected.getMessage().contains("reserved suffix"), rejected.getMessage());
		Assertions.assertThrows(IllegalArgumentException.class, () -> storage.getAssociatedDocumentOutputStream("docLegacy", "x.metadata", 1L, new Document()));

		// a sidecar written by an older server in relaxed JSON still reads
		try (Stream<Path> walk = Files.walk(dataPath)) {
			Path sidecar = walk.filter(p -> p.getFileName().toString().equals("notes.txt.metadata")).findFirst().orElseThrow();
			Files.writeString(sidecar, "{\"pages\": 12, \"_tstamp_\": 5}");
		}
		AssociatedDocument fetched = storage.getAssociatedDocument("docLegacy", "notes.txt", FetchType.META);
		Assertions.assertEquals(5L, fetched.getTimestamp());
		Assertions.assertEquals(12, ZuliaUtil.byteStringToMongoDocument(fetched.getMetadata()).getInteger("pages"));
	}

	@Test
	public void longMetadataValuesSurviveTheSidecarInBothLayouts() throws Exception {
		for (int version : new int[] { ZuliaIndexVersion.CURRENT, ZuliaIndexVersion.ASSOCIATED_FILE_LAYOUT_V2 - 1 }) {
			FileDocumentStorage storage = new FileDocumentStorage(dataPath.resolve("v" + version).toString(), "testIndex", version);
			Document metadata = new Document("pages", 12L).append("ratio", 0.5).append("nested", new Document("n", 7L));
			AssociatedDocument stored = AssociatedDocument.newBuilder().setDocumentUniqueId("docLong").setFilename("paper.pdf").setIndexName("testIndex")
					.setDocument(ByteString.copyFrom("x".getBytes(StandardCharsets.UTF_8))).setMetadata(ZuliaUtil.mongoDocumentToByteString(metadata)).setTimestamp(9L)
					.build();
			storage.storeAssociatedDocument(stored);

			Document fetched = ZuliaUtil.byteStringToMongoDocument(storage.getAssociatedDocument("docLong", "paper.pdf", FetchType.META).getMetadata());
			Assertions.assertEquals(12L, fetched.getLong("pages"), "layout " + version);
			Assertions.assertEquals(0.5, fetched.getDouble("ratio"), "layout " + version);
			Assertions.assertEquals(7L, fetched.get("nested", Document.class).getLong("n"), "layout " + version);
			Assertions.assertFalse(fetched.containsKey("_tstamp_"));
		}
	}

	@Test
	public void newLayoutSpreadsSequentialIdsAcrossBuckets() throws Exception {
		FileDocumentStorage current = new FileDocumentStorage(dataPath.resolve("current").toString(), "testIndex", ZuliaIndexVersion.CURRENT);
		FileDocumentStorage legacy = new FileDocumentStorage(dataPath.resolve("legacy").toString(), "testIndex", ZuliaIndexVersion.ASSOCIATED_FILE_LAYOUT_V2 - 1);
		for (int i = 30000000; i < 30000400; i++) {
			current.storeAssociatedDocument(buildDocument(Integer.toString(i), "a.txt"));
			legacy.storeAssociatedDocument(buildDocument(Integer.toString(i), "a.txt"));
		}
		long currentTopDirs = countTopBuckets(dataPath.resolve("current"));
		long legacyTopDirs = countTopBuckets(dataPath.resolve("legacy"));
		Assertions.assertTrue(currentTopDirs > 300, "400 sequential ids should land in about 380 of the 4096 top buckets, reached " + currentTopDirs);
		Assertions.assertTrue(legacyTopDirs < 10, "the legacy layout piles sequential ids into a few top buckets, used " + legacyTopDirs);
		for (int i = 30000000; i < 30000400; i++) {
			Assertions.assertEquals(List.of("a.txt"), current.getAssociatedFilenames(Integer.toString(i)));
		}
	}

	private static long countTopBuckets(Path root) throws Exception {
		try (Stream<Path> dirs = Files.list(root.resolve("files").resolve("testIndex"))) {
			return dirs.filter(Files::isDirectory).count();
		}
	}

	private static AssociatedDocument buildDocument(String uniqueId, String filename) {
		return AssociatedDocument.newBuilder().setDocumentUniqueId(uniqueId).setFilename(filename).setIndexName("testIndex")
				.setDocument(ByteString.copyFrom(filename.getBytes(StandardCharsets.UTF_8))).setTimestamp(1L).build();
	}
}
