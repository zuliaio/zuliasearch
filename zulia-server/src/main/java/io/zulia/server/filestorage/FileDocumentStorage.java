package io.zulia.server.filestorage;

import com.google.protobuf.ByteString;
import io.zulia.message.ZuliaBase;
import io.zulia.message.ZuliaBase.AssociatedDocument;
import io.zulia.message.ZuliaQuery.FetchType;
import io.zulia.rest.dto.AssociatedMetadataDTO;
import io.zulia.server.config.ZuliaConfig;
import io.zulia.server.index.ZuliaIndexVersion;
import io.zulia.util.ShardUtil;
import io.zulia.util.ZuliaUtil;
import io.zulia.util.document.DocumentHelper;
import org.bson.Document;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FileDocumentStorage implements DocumentStorage {

	/** On-disk layout of associated documents, fixed by the index creation version. */
	public enum FileLayout {
		/** Buckets from the high-order digits of String.hashCode, {@code <filename>.metadata} beside each file, so that suffix is reserved. */
		LEGACY,
		/** Three decimal bucket levels of 1000 from the low-order end of the 64-bit djb2 hash, {@code .meta/<filename>} under the unique id directory, any filename allowed. */
		DJB2_META_DIR;

		public static FileLayout forIndexVersion(int createdIndexVersion) {
			return createdIndexVersion >= ZuliaIndexVersion.ASSOCIATED_FILE_LAYOUT_V2 ? DJB2_META_DIR : LEGACY;
		}
	}

	private static final String TIMESTAMP = "_tstamp_";
	private static final String META_DIR = ".meta";
	private static final String LEGACY_SUFFIX = ".metadata";
	// extended mode keeps int64, double and Decimal128 typed on disk, the parser accepts relaxed and extended alike
	private static final JsonWriterSettings SIDECAR_JSON = JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).build();

	private final String indexName;
	private final Path indexRoot;
	private final FileLayout layout;

	public FileDocumentStorage(ZuliaConfig zuliaConfig, String indexName, int createdIndexVersion) {
		this(zuliaConfig.getDataPath(), indexName, createdIndexVersion);
	}

	public FileDocumentStorage(String dataPath, String indexName, int createdIndexVersion) {
		this.indexName = indexName;
		this.indexRoot = Path.of(dataPath, "files", indexName).toAbsolutePath().normalize();
		this.layout = FileLayout.forIndexVersion(createdIndexVersion);
	}

	public FileLayout getFileLayout() {
		return layout;
	}

	@Override
	public void storeAssociatedDocument(AssociatedDocument doc) throws Exception {
		Path pathForUniqueId = createPathForUniqueIdIfNotExists(doc.getDocumentUniqueId());
		checkStorable(doc.getFilename());
		Files.write(fileIn(pathForUniqueId, doc.getFilename()), doc.getDocument().toByteArray());

		Document metadata = ZuliaUtil.byteArrayToMongoDocument(doc.getMetadata().toByteArray());
		metadata.put(TIMESTAMP, doc.getTimestamp());
		writeSidecar(pathForUniqueId, doc.getFilename(), metadata);
	}

	@Override
	public OutputStream getAssociatedDocumentOutputStream(String uniqueId, String fileName, long timestamp, Document metadataMap) throws Exception {
		Path pathForUniqueId = createPathForUniqueIdIfNotExists(uniqueId);
		checkStorable(fileName);
		metadataMap.put(TIMESTAMP, timestamp);
		writeSidecar(pathForUniqueId, fileName, metadataMap);

		return new FileOutputStream(fileIn(pathForUniqueId, fileName).toFile());

	}

	@Override
	public List<AssociatedDocument> getAssociatedMetadataForUniqueId(String uniqueId, FetchType fetchType) throws Exception {

		if (!FetchType.NONE.equals(fetchType)) {
			List<AssociatedDocument> associatedDocuments = new ArrayList<>();
			List<String> fileNames = getAssociatedFilenames(uniqueId);
			for (String fileName : fileNames) {
				associatedDocuments.add(getAssociatedDocument(uniqueId, fileName, fetchType));
			}
			return associatedDocuments;
		}
		return Collections.emptyList();
	}

	@Override
	public AssociatedDocument getAssociatedDocument(String uniqueId, String filename, FetchType fetchType) throws Exception {
		if (!FetchType.NONE.equals(fetchType)) {
			AssociatedDocument.Builder aBuilder = AssociatedDocument.newBuilder();
			aBuilder.setFilename(filename);

			Document metadata = new Document();
			Path pathForUniqueId = pathForUniqueId(uniqueId);
			Path metadataPath = metadataIn(pathForUniqueId, filename);
			if (Files.exists(metadataPath)) {
				String metadataJson = Files.readString(metadataPath);
				metadata = Document.parse(metadataJson);
				long timestamp = DocumentHelper.getAsLong(metadata, TIMESTAMP, 0L);
				metadata.remove(TIMESTAMP);
				aBuilder.setTimestamp(timestamp);
			}

			aBuilder.setDocumentUniqueId(uniqueId);
			aBuilder.setMetadata(ZuliaUtil.mongoDocumentToByteString(metadata));

			if (FetchType.FULL.equals(fetchType)) {
				byte[] fileBytes = Files.readAllBytes(fileIn(pathForUniqueId, filename));
				aBuilder.setDocument(ByteString.copyFrom(fileBytes));
			}
			aBuilder.setIndexName(indexName);
			return aBuilder.build();
		}
		return null;
	}

	@Override
	public Stream<AssociatedMetadataDTO> getAssociatedMetadataForQuery(Document query) {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public InputStream getAssociatedDocumentStream(String uniqueId, String filename) throws FileNotFoundException {
		Path file = fileIn(pathForUniqueId(uniqueId), filename);
		return new BufferedInputStream(new FileInputStream(file.toFile()));
	}

	@Override
	public List<String> getAssociatedFilenames(String uniqueId) throws Exception {
		Path p = pathForUniqueId(uniqueId);
		if (!Files.exists(p)) {
			return Collections.emptyList();
		}
		// every store writes a sidecar with its document, so a document is exactly an entry whose
		// sidecar exists. This also hides sidecars orphaned by deletes from before the sidecar was
		// removed with its document.
		try (Stream<Path> files = Files.list(p)) {
			Set<String> names = files.map(Path::getFileName).map(Path::toString).collect(Collectors.toSet());
			return switch (layout) {
				case LEGACY -> names.stream().filter(name -> names.contains(name + LEGACY_SUFFIX)).sorted().collect(Collectors.toList());
				case DJB2_META_DIR -> names.stream().filter(name -> !META_DIR.equals(name) && Files.exists(p.resolve(META_DIR).resolve(name))).sorted()
						.collect(Collectors.toList());
			};
		}
	}

	@Override
	public void deleteAssociatedDocument(String uniqueId, String fileName) throws IOException {
		Path pathForUniqueId = pathForUniqueId(uniqueId);
		Files.deleteIfExists(fileIn(pathForUniqueId, fileName));
		Files.deleteIfExists(metadataIn(pathForUniqueId, fileName));
	}

	@Override
	public void deleteAssociatedDocuments(String uniqueId) throws IOException {
		deletePath(pathForUniqueId(uniqueId));
	}

	@Override
	public void registerExternalDocument(ZuliaBase.ExternalDocument registration) {
		throw new UnsupportedOperationException("Cannot register a locally stored file with Zulia.");
	}

	@Override
	public void drop() throws Exception {
		deletePath(indexRoot);
	}

	@Override
	public void deleteAllDocuments() throws Exception {
		deletePath(indexRoot);
		Files.createDirectories(indexRoot);
	}

	private Path createPathForUniqueIdIfNotExists(String uniqueId) throws Exception {
		Path pathForUniqueId = pathForUniqueId(uniqueId);
		File f = pathForUniqueId.toFile();
		if (!f.exists()) {
			boolean created = f.mkdirs();
			if (!created) {
				if (!f.exists()) {
					throw new Exception(
							"Failed to create directory for associated document with unique id <" + uniqueId + "> for indexName <" + indexName + "> in path <"
									+ pathForUniqueId + ">");
				}
				if (!f.isDirectory()) {
					throw new Exception(
							"Failed to create directory for associated document with unique id <" + uniqueId + "> for indexName <" + indexName + "> in path <"
									+ pathForUniqueId + "> because path exist and is not a directory");
				}
			}
		}
		return pathForUniqueId;
	}

	private void deletePath(Path pathToBeDeleted) throws IOException {
		if (!Files.exists(pathToBeDeleted)) {
			return;
		}
		Files.walkFileTree(pathToBeDeleted, new SimpleFileVisitor<>() {
			@Override
			public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
				Files.delete(dir);
				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
				Files.delete(file);
				return FileVisitResult.CONTINUE;
			}
		});
	}

	/** Directory holding the associated documents of one unique id, verified to lie inside the id's own hash bucket. */
	private Path pathForUniqueId(String uniqueId) {
		// anchoring on the bucket rather than the index root keeps a ".." id from resolving to a
		// bucket shared with other ids, where a delete-all would remove their files too
		Path bucket = indexRoot.resolve(getBucketForUniqueId(uniqueId));
		Path raw = bucket.resolve(uniqueId);
		// a "." or ".." segment can only ever redirect the id into another id's directory (with a hash
		// collision, which String.hashCode makes trivial to forge), so the id must resolve to itself
		if (!raw.normalize().equals(raw)) {
			throw new IllegalArgumentException(
					"Associated document unique id <" + uniqueId + "> contains a relative path segment, which is not allowed for index <" + indexName + ">");
		}
		return contained(bucket, uniqueId, "unique id", uniqueId);
	}

	/** The document file for a filename, verified to lie under the unique id's directory. */
	private Path fileIn(Path pathForUniqueId, String filename) {
		if (filename == null || filename.isBlank()) {
			throw new IllegalArgumentException("Associated document filename must not be blank for index <" + indexName + ">");
		}
		return contained(pathForUniqueId, filename, "filename", filename);
	}

	/** The metadata sidecar for a filename, subject to the same containment check as the document. */
	private Path metadataIn(Path pathForUniqueId, String filename) {
		Path file = fileIn(pathForUniqueId, filename);
		return switch (layout) {
			case LEGACY -> file.resolveSibling(file.getFileName() + LEGACY_SUFFIX);
			case DJB2_META_DIR -> contained(pathForUniqueId.resolve(META_DIR), filename, "filename", filename);
		};
	}

	private void writeSidecar(Path pathForUniqueId, String filename, Document metadata) throws IOException {
		Path sidecar = metadataIn(pathForUniqueId, filename);
		Files.createDirectories(sidecar.getParent());
		Files.write(sidecar, Collections.singleton(metadata.toJson(SIDECAR_JSON)));
	}

	/** Under the legacy layout a document named like a sidecar would shadow another file's metadata. */
	private void checkStorable(String filename) {
		if (layout == FileLayout.LEGACY && filename != null && filename.endsWith(LEGACY_SUFFIX)) {
			throw new IllegalArgumentException(
					"Associated document filename <" + filename + "> ends with the reserved suffix " + LEGACY_SUFFIX + " for index <" + indexName + ">");
		}
	}

	// The unique id and filename arrive as free strings on the store request and Path.resolve does not
	// normalize, so a ".." segment in either would let a store, fetch or delete escape the data directory.
	// Values containing a separator are allowed (the other backends accept them), only escaping is rejected.
	private Path contained(Path base, String relative, String what, String value) {
		Path resolved;
		try {
			resolved = base.resolve(relative).normalize();
		}
		catch (InvalidPathException e) {
			throw new IllegalArgumentException(
					"Associated document " + what + " <" + value + "> is not a valid path for index <" + indexName + ">: " + e.getReason(), e);
		}
		if (!resolved.startsWith(base) || resolved.equals(base)) {
			throw new IllegalArgumentException(
					"Associated document " + what + " <" + value + "> resolves outside the storage directory for index <" + indexName + ">");
		}
		return resolved;
	}

	private String getBucketForUniqueId(String uniqueId) {
		return switch (layout) {
			case LEGACY -> legacyBucket(uniqueId);
			case DJB2_META_DIR -> djb2Bucket(uniqueId);
		};
	}

	private static String legacyBucket(String uniqueId) {
		// Integer.toHexString drops leading zeros. Hashes shorter than five digits crashed the
		// substring math below before any file could be stored, so padding only those to five
		// keeps every layout that ever held data unchanged.
		String hexHash = Integer.toHexString(uniqueId.hashCode());
		if (hexHash.length() < 5) {
			hexHash = "0".repeat(5 - hexHash.length()) + hexHash;
		}
		String piece1 = hexHash.substring(0, 2);
		String piece2 = hexHash.substring(2, 5);
		String piece3 = hexHash.substring(5);
		return piece1 + File.separator + piece2 + File.separator + piece3;
	}

	private static String djb2Bucket(String uniqueId) {
		// two levels of 4096 directories from the low-order end of the hash, which is the well mixed
		// end of a multiply-and-add hash. Short or sequential ids leave the high-order digits nearly
		// constant, which piled every id into a few directories under the legacy layout. 16.7 million
		// leaf buckets keep any directory under a few thousand entries up to hundreds of millions of
		// ids, with one level fewer to traverse than the legacy layout.
		String hex = String.format("%016x", ShardUtil.djb2Hash(uniqueId));
		return hex.substring(13, 16) + File.separator + hex.substring(10, 13);
	}

}
