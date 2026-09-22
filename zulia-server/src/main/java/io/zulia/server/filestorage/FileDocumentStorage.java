package io.zulia.server.filestorage;

import com.google.protobuf.ByteString;
import io.zulia.message.ZuliaBase;
import io.zulia.message.ZuliaBase.AssociatedDocument;
import io.zulia.message.ZuliaQuery.FetchType;
import io.zulia.rest.dto.AssociatedMetadataDTO;
import io.zulia.server.config.ZuliaConfig;
import io.zulia.util.ZuliaUtil;
import io.zulia.util.document.DocumentHelper;
import org.bson.Document;

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
	private static final String TIMESTAMP = "_tstamp_";
	private final String indexName;
	private final Path indexRoot;

	public FileDocumentStorage(ZuliaConfig zuliaConfig, String indexName) {
		this(zuliaConfig.getDataPath(), indexName);
	}

	public FileDocumentStorage(String dataPath, String indexName) {
		this.indexName = indexName;
		this.indexRoot = Path.of(dataPath, "files", indexName).toAbsolutePath().normalize();
	}

	@Override
	public void storeAssociatedDocument(AssociatedDocument doc) throws Exception {
		Path pathForUniqueId = createPathForUniqueIdIfNotExists(doc.getDocumentUniqueId());
		Files.write(fileIn(pathForUniqueId, doc.getFilename()), doc.getDocument().toByteArray());

		Document metadata = ZuliaUtil.byteArrayToMongoDocument(doc.getMetadata().toByteArray());
		metadata.put(TIMESTAMP, doc.getTimestamp());
		Files.write(metadataIn(pathForUniqueId, doc.getFilename()), Collections.singleton(metadata.toJson()));
	}

	@Override
	public OutputStream getAssociatedDocumentOutputStream(String uniqueId, String fileName, long timestamp, Document metadataMap) throws Exception {
		Path pathForUniqueId = createPathForUniqueIdIfNotExists(uniqueId);
		metadataMap.put(TIMESTAMP, timestamp);
		Files.write(metadataIn(pathForUniqueId, fileName), Collections.singleton(metadataMap.toJson()));

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
		if (Files.exists(p)) {
			try (Stream<Path> files = Files.list(p)) {
				// Every store writes a metadata sidecar next to the document, so a document file is
				// exactly an entry whose ".metadata" sibling exists. This also hides sidecars orphaned
				// by deletes from before the sidecar was removed with its document.
				Set<String> names = files.map(Path::getFileName).map(Path::toString).collect(Collectors.toSet());
				return names.stream().filter(name -> names.contains(name + ".metadata")).sorted().collect(Collectors.toList());
			}
		}
		return Collections.emptyList();
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

	/** Directory holding the associated documents of one unique id, verified to lie under the index root. */
	private Path pathForUniqueId(String uniqueId) {
		return contained(indexRoot, getPathToUniqueId(uniqueId), "unique id", uniqueId);
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
		return file.resolveSibling(file.getFileName() + ".metadata");
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

	private static String getPathToUniqueId(String uniqueId) {
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
		return piece1 + File.separator + piece2 + File.separator + piece3 + File.separator + uniqueId;
	}

}
