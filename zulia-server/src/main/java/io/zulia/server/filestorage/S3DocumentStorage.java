package io.zulia.server.filestorage;

import com.google.protobuf.ByteString;
import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.internal.HexUtils;
import io.zulia.message.ZuliaBase.AssociatedDocument;
import io.zulia.message.ZuliaQuery.FetchType;
import io.zulia.server.config.cluster.S3Config;
import io.zulia.server.filestorage.io.S3OutputStream;
import io.zulia.util.ZuliaUtil;
import org.bson.Document;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;
import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;

public class S3DocumentStorage implements DocumentStorage {
	private static final String TIMESTAMP = "_tstamp_";
	private static final String DOCUMENT_UNIQUE_ID_KEY = "_uid_";
	private static final String FILE_UNIQUE_ID_KEY = "_fid_";
	private static final String COLLECTION = "associatedFiles.info";
	public static final String FILENAME = "filename";

	private final MongoClient client;
	private final String indexName;
	private final String dbName;
	private final boolean sharded;
	private final String bucket;
	private final S3Client s3;
	private final String region;

	public S3DocumentStorage(MongoClient mongoClient, String indexName, String dbName, boolean sharded, S3Config s3Config) {
		if (null == s3Config)
			throw new IllegalArgumentException("Must provide the s3 config section");
		if (null == s3Config.getS3BucketName())
			throw new IllegalArgumentException("Must provide the S3 bucket that is going to be used to store content");
		if (null == s3Config.getRegion())
			throw new IllegalArgumentException("Must provide the region the s3 bucket lives in.");
		this.bucket = s3Config.getS3BucketName();
		this.region = s3Config.getRegion();
		this.client = mongoClient;
		this.indexName = indexName;
		this.dbName = dbName;
		this.sharded = sharded;
		AwsCredentialsProviderChain credentialsProvider = AwsCredentialsProviderChain.builder()
				.credentialsProviders(
						InstanceProfileCredentialsProvider.builder().build(),
						ContainerCredentialsProvider.builder().build(),
						EnvironmentVariableCredentialsProvider.create(),
						SystemPropertyCredentialsProvider.create(),
						ProfileCredentialsProvider.builder().build()
				)
				.build();

		this.s3 = S3Client.builder().region(Region.of(this.region)).credentialsProvider(credentialsProvider).build();

		ForkJoinPool.commonPool().execute(() -> {
			MongoDatabase db = client.getDatabase(dbName);
			MongoCollection<Document> coll = db.getCollection(COLLECTION);
			coll.createIndex(new Document("metadata." + DOCUMENT_UNIQUE_ID_KEY, 1), new IndexOptions().background(true));
			coll.createIndex(new Document("metadata." + FILE_UNIQUE_ID_KEY, 1), new IndexOptions().background(true));
			if (sharded) {
				MongoDatabase adminDb = client.getDatabase("admin");
				Document enableCommand = new Document();
				enableCommand.put("enablesharding", dbName);
				adminDb.runCommand(enableCommand);

				Document shardCommand = new Document();
				MongoCollection<Document> collection = db.getCollection(COLLECTION);
				shardCommand.put("shardcollection", collection.getNamespace().getFullName());
				shardCommand.put("key", new BasicDBObject("_id", 1));
				adminDb.runCommand(shardCommand);
			}
		});
	}

	@Override
	public void storeAssociatedDocument(AssociatedDocument doc) throws Exception {
		byte[] bytes = doc.getDocument().toByteArray();

		Document TOC = parseAssociated(doc, (long) bytes.length);

		String hex = HexUtils.hexMD5(doc.getFilename().getBytes(StandardCharsets.UTF_8));
		String key = String.join("/", indexName, doc.getDocumentUniqueId(), String.join(".", hex, "sz"));

		Document s3Location = new Document();
		s3Location.put("bucket", bucket);
		s3Location.put("region", region);
		s3Location.put("key", key);
		TOC.put("s3", s3Location);

		PutObjectRequest req = PutObjectRequest.builder().bucket(bucket).key(key).contentLength((long) bytes.length).build();

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		SnappyOutputStream os = new SnappyOutputStream(baos);
		os.write(bytes);
		os.flush();
		byte[] compressed = baos.toByteArray();
		os.close();

		s3.putObject(req, RequestBody.fromBytes(compressed));
		client.getDatabase(dbName).getCollection(COLLECTION).insertOne(TOC);
	}

	@Override
	public List<AssociatedDocument> getAssociatedDocuments(String uniqueId, FetchType fetchType) throws Exception {
		if (FetchType.NONE.equals(fetchType))
			return Collections.emptyList();
		FindIterable<Document> found = client.getDatabase(dbName).getCollection(COLLECTION).find(Filters.eq("metadata." + DOCUMENT_UNIQUE_ID_KEY, uniqueId));

		List<AssociatedDocument> docs = new ArrayList<>();
		for (Document doc : found) {
			docs.add(parseTOC(doc));
		}
		return docs;
	}

	@Override
	public AssociatedDocument getAssociatedDocument(String uniqueId, String filename, FetchType fetchType) throws Exception {
		if (!FetchType.NONE.equals(fetchType)) {
			String uid = String.join("-", uniqueId, filename);
			FindIterable<Document> found = client.getDatabase(dbName).getCollection(COLLECTION).find(Filters.eq("metadata." + FILE_UNIQUE_ID_KEY, uid));
			Document doc = found.first();
			if (null != doc) {
				return parseTOC(doc);
			}
		}
		return null;
	}

	@Override
	public void getAssociatedDocuments(Writer outputstream, Document filter) throws Exception {
		FindIterable<Document> found = client.getDatabase(dbName).getCollection(COLLECTION).find(filter);
		outputstream.write("{\n");
		outputstream.write(" \"associatedDocs\": [\n");

		boolean first = true;
		for (Document doc : found) {
			if (first) {
				first = false;
			} else {
				outputstream.write(",\n");
			}

			Document metadata = doc.get("metadata", Document.class);

			String uniqueId = metadata.getString(DOCUMENT_UNIQUE_ID_KEY);
			outputstream.write("  { \"uniqueId\": \"" + uniqueId + "\", ");

			String filename = doc.getString("filename");
			outputstream.write("\"filename\": \"" + filename + "\", ");

			Date uploadDate = doc.getDate("uploadDate");
			outputstream.write("\"uploadDate\": {\"$date\":" + uploadDate.getTime() + "}");

			metadata.remove(TIMESTAMP);
			metadata.remove(DOCUMENT_UNIQUE_ID_KEY);
			metadata.remove(FILE_UNIQUE_ID_KEY);

			if (!metadata.isEmpty()) {
				String metaJson = metadata.toJson();
				String metaString = ", \"meta\": " + metaJson;
				outputstream.write(metaString);
			}
			outputstream.write(" }");
		}
		outputstream.write("\n ]\n}");
	}

	@Override
	public OutputStream getAssociatedDocumentOutputStream(String uniqueId, String fileName, long timestamp, Document metadataMap) throws Exception {
		deleteAssociatedDocument(uniqueId, fileName);

		Document TOC = new Document();
		TOC.put("filename", fileName);
		TOC.put("metadata", metadataMap);
		metadataMap.put(TIMESTAMP, timestamp);
		metadataMap.put(DOCUMENT_UNIQUE_ID_KEY, uniqueId);
		metadataMap.put(FILE_UNIQUE_ID_KEY, String.join("-", uniqueId, fileName));

		String hex = HexUtils.hexMD5(fileName.getBytes(StandardCharsets.UTF_8));
		String key = String.join("/", indexName, uniqueId, String.join(".", hex, "sz"));

		Document s3Location = new Document();
		s3Location.put("bucket", bucket);
		s3Location.put("region", region);
		s3Location.put("key", key);
		TOC.put("s3", s3Location);

		client.getDatabase(dbName).getCollection(COLLECTION).insertOne(TOC);

		return new SnappyOutputStream(new S3OutputStream(s3, bucket, key));
	}

	@Override
	public InputStream getAssociatedDocumentStream(String uniqueId, String filename) throws Exception {
		FindIterable<Document> found = client.getDatabase(dbName).getCollection(COLLECTION).find(Filters.eq("metadata." + FILE_UNIQUE_ID_KEY, String.join("-", uniqueId, filename)));
		Document doc = found.first();
		if (null != doc) {
			Document s3Info = doc.get("s3", Document.class);
			GetObjectRequest gor = GetObjectRequest.builder().bucket(s3Info.getString("bucket")).key(s3Info.getString("key")).build();
			ResponseInputStream<GetObjectResponse> results = s3.getObject(gor);
			return new BufferedInputStream(new SnappyInputStream(results));
		}
		return null;
	}

	@Override
	public List<String> getAssociatedFilenames(String uniqueId) throws Exception {
		FindIterable<Document> found = client.getDatabase(dbName).getCollection(COLLECTION).find(Filters.eq("metadata." + DOCUMENT_UNIQUE_ID_KEY, uniqueId));
		List<String> files = new ArrayList<>();
		found.map(doc -> doc.getString(FILENAME)).forEach(files::add);
		return files;
	}

	@Override
	public void deleteAssociatedDocument(String uniqueId, String fileName) throws Exception {
		FindIterable<Document> found = client.getDatabase(dbName).getCollection(COLLECTION).find(Filters.eq("metadata." + FILE_UNIQUE_ID_KEY, String.join("-", uniqueId, fileName)));
		Document doc = found.first();
		if (null != doc) {
			client.getDatabase(dbName).getCollection(COLLECTION).deleteOne(Filters.eq("_id", doc.getObjectId("_id")));
			Document s3Info = doc.get("s3", Document.class);
			DeleteObjectRequest dor = DeleteObjectRequest.builder().bucket(s3Info.getString("bucket")).key(s3Info.getString("key")).build();
			s3.deleteObject(dor);
		}
	}

	@Override
	public void deleteAssociatedDocuments(String uniqueId) throws Exception {
		FindIterable<Document> found = client.getDatabase(dbName).getCollection(COLLECTION).find(Filters.eq("metadata." + DOCUMENT_UNIQUE_ID_KEY, uniqueId));
		for (Document doc : found) {
			client.getDatabase(dbName).getCollection(COLLECTION).deleteOne(Filters.eq("_id", doc.getObjectId("_id")));
			Document s3Info = doc.get("s3", Document.class);
			DeleteObjectRequest dor = DeleteObjectRequest.builder().bucket(s3Info.getString("bucket")).key(s3Info.getString("key")).build();
			s3.deleteObject(dor);
		}
	}

	@Override
	public void drop() throws Exception {
		FindIterable<Document> found = client.getDatabase(dbName).getCollection(COLLECTION).find();
		deleteAllKeys(found);
		client.getDatabase(dbName).drop();
	}

	@Override
	public void deleteAllDocuments() throws Exception {
		FindIterable<Document> found = client.getDatabase(dbName).getCollection(COLLECTION).find();
		deleteAllKeys(found);
		client.getDatabase(dbName).getCollection(COLLECTION).drop();
	}

	private void deleteAllKeys(FindIterable<Document> found) {
		List<String> keyBatch = new ArrayList<>(1000);
		for (Document doc : found) {
			Document s3Info = doc.get("s3", Document.class);
			keyBatch.add(s3Info.getString("key"));
			if (keyBatch.size() % 1000 == 0) {
				deleteKeys(keyBatch);
				keyBatch.clear();
			}
		}
		if (keyBatch.size() > 0) {
			deleteKeys(keyBatch);
			keyBatch.clear();
		}
	}

	private void deleteKeys(List<String> keyBatch) {
		DeleteObjectsRequest dor = DeleteObjectsRequest.builder()
				.bucket(bucket)
				.bypassGovernanceRetention(true)
				.delete(Delete.builder()
						.objects(
								keyBatch.stream()
										.map(s -> ObjectIdentifier.builder().key(s).build())
										.collect(Collectors.toList())
						).build())
				.build();
		s3.deleteObjects(dor);
	}

	private Document parseAssociated(AssociatedDocument doc, Long length) {
		Document metadata;
		if (!doc.getMetadata().isEmpty()) {
			metadata = ZuliaUtil.byteArrayToMongoDocument(doc.getMetadata().toByteArray());
		} else {
			metadata = new Document();
		}

		metadata.put(TIMESTAMP, doc.getTimestamp());
		metadata.put(FILE_UNIQUE_ID_KEY, String.join("-", doc.getDocumentUniqueId(), doc.getFilename()));
		metadata.put(DOCUMENT_UNIQUE_ID_KEY, doc.getDocumentUniqueId());

		Document TOC = new Document();
		TOC.put("metadata", metadata);
		TOC.put(FILENAME, doc.getFilename());
		TOC.put("length", length);
		TOC.put("uploadDate", Instant.now());
		return TOC;
	}

	private AssociatedDocument parseTOC(Document doc) throws IOException {
		AssociatedDocument.Builder aBuilder = AssociatedDocument.newBuilder();
		aBuilder.setFilename(doc.getString(FILENAME));

		Document meta = doc.get("metadata", Document.class);
		aBuilder.setDocumentUniqueId(meta.getString(DOCUMENT_UNIQUE_ID_KEY));
		aBuilder.setTimestamp(meta.getLong(TIMESTAMP));
		aBuilder.setIndexName(indexName);
		meta.remove(TIMESTAMP);
		meta.remove(DOCUMENT_UNIQUE_ID_KEY);
		meta.remove(FILE_UNIQUE_ID_KEY);
		aBuilder.setMetadata(ZuliaUtil.mongoDocumentToByteString(meta));

		Document s3Info = doc.get("s3", Document.class);
		GetObjectRequest gor = GetObjectRequest.builder()
				.bucket(s3Info.getString("bucket"))
				.key(s3Info.getString("key"))
				.build();
		ResponseInputStream<GetObjectResponse> results = s3.getObject(gor);
		InputStream compression = new SnappyInputStream(results);
		try (compression) {
			aBuilder.setDocument(ByteString.readFrom(compression));
		}

		return aBuilder.build();
	}
}
