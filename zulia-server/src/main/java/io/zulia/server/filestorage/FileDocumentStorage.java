package io.zulia.server.filestorage;

import com.google.protobuf.ByteString;
import io.zulia.message.ZuliaBase.AssociatedDocument;
import io.zulia.message.ZuliaQuery.FetchType;
import io.zulia.server.config.ZuliaConfig;
import io.zulia.util.ZuliaUtil;
import org.bson.Document;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Writer;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FileDocumentStorage implements DocumentStorage {
	private static final String TIMESTAMP = "_tstamp_";

	@SuppressWarnings("unused")
	private final static Logger log = Logger.getLogger(FileDocumentStorage.class.getSimpleName());
	private final String indexName;
	private final String filesPath;

	public FileDocumentStorage(ZuliaConfig zuliaConfig, String indexName) {
		this(zuliaConfig.getDataPath(), indexName);
	}

	public FileDocumentStorage(String dataPath, String indexName) {
		this.indexName = indexName;
		this.filesPath = dataPath + File.separator + "files";
	}

	@Override
	public void storeAssociatedDocument(AssociatedDocument doc) throws Exception {
		String pathForUniqueId = createPathForUniqueIdIfNotExists(doc.getDocumentUniqueId());
		Files.write(Path.of(pathForUniqueId, doc.getFilename()), doc.getDocument().toByteArray());

		Document metadata = ZuliaUtil.byteArrayToMongoDocument(doc.getMetadata().toByteArray());
		metadata.put(TIMESTAMP, doc.getTimestamp());
		Files.write(Path.of(pathForUniqueId, doc.getFilename() + ".metadata"), Collections.singleton(doc.getMetadata().toString()));
	}

	@Override
	public OutputStream getAssociatedDocumentOutputStream(String uniqueId, String fileName, long timestamp, Document metadataMap) throws Exception {
		String pathForUniqueId = createPathForUniqueIdIfNotExists(uniqueId);
		metadataMap.put(TIMESTAMP, timestamp);
		Files.write(Path.of(pathForUniqueId, fileName + ".metadata"), Collections.singleton(metadataMap.toJson()));

		return new FileOutputStream(Path.of(pathForUniqueId, fileName).toFile());

	}

	@Override
	public List<AssociatedDocument> getAssociatedDocuments(String uniqueId, FetchType fetchType) throws Exception {

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
			String pathForUniqueId = getFullPathToUniqueId(uniqueId);
			Path metadataPath = Path.of(pathForUniqueId, filename + ".metadata");
			if (metadataPath.toFile().exists()) {
				String metadataJson = Files.readString(metadataPath);
				metadata = Document.parse(metadataJson);
				long timestamp = (long) metadata.remove(TIMESTAMP);
				aBuilder.setTimestamp(timestamp);
			}

			aBuilder.setDocumentUniqueId(uniqueId);
			aBuilder.setMetadata(ZuliaUtil.mongoDocumentToByteString(metadata));

			if (FetchType.FULL.equals(fetchType)) {
				byte[] fileBytes = Files.readAllBytes(Path.of(pathForUniqueId, filename));
				aBuilder.setDocument(ByteString.copyFrom(fileBytes));
			}
			aBuilder.setIndexName(indexName);
			return aBuilder.build();
		}
		return null;
	}

	@Override
	public void getAssociatedDocuments(Writer writer, Document filter) throws Exception {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public InputStream getAssociatedDocumentStream(String uniqueId, String filename) throws FileNotFoundException {
		String pathForUniqueId = getFullPathToUniqueId(uniqueId);
		return new BufferedInputStream(new FileInputStream(Path.of(pathForUniqueId, filename).toFile()));
	}

	@Override
	public List<String> getAssociatedFilenames(String uniqueId) throws Exception {
		String pathForUniqueId = getFullPathToUniqueId(uniqueId);

		Path p = Path.of(pathForUniqueId);
		if (Files.exists(p)) {
			Stream<Path> list = Files.list(p);
			return list.map(Path::toFile).map(File::getName).collect(Collectors.toList());
		}
		return Collections.emptyList();
	}

	@Override
	public void deleteAssociatedDocument(String uniqueId, String fileName) throws IOException {
		String pathForUniqueId = getFullPathToUniqueId(uniqueId);
		Files.delete(Path.of(pathForUniqueId, fileName));
	}

	@Override
	public void deleteAssociatedDocuments(String uniqueId) throws IOException {
		String pathForUniqueId = getFullPathToUniqueId(uniqueId);
		deletePath(Path.of(pathForUniqueId));
	}

	@Override
	public void drop() throws Exception {
		Path p = Path.of(filesPath, indexName);
		if (Files.exists(p)) {
			deletePath(p);
		}
	}

	@Override
	public void deleteAllDocuments() throws Exception {
		deletePath(Path.of(filesPath, indexName));
		new File(filesPath + File.separator + indexName).mkdirs();
	}

	private String createPathForUniqueIdIfNotExists(String uniqueId) throws Exception {
		String pathForUniqueId = getFullPathToUniqueId(uniqueId);
		File f = new File(pathForUniqueId);
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

	private String getFullPathToUniqueId(String uniqueId) {
		return filesPath + File.separator + indexName + File.separator + getPathToUniqueId(uniqueId);
	}

	private static String getPathToUniqueId(String uniqueId) {
		String hexHash = Integer.toHexString(uniqueId.hashCode());
		String piece1 = hexHash.substring(0, 2);
		String piece2 = hexHash.substring(2, 5);
		String piece3 = hexHash.substring(5);
		return piece1 + File.separator + piece2 + File.separator + piece3 + File.separator + uniqueId;
	}

}
