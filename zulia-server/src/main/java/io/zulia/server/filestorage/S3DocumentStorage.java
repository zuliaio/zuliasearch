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
import io.zulia.message.ZuliaBase;
import io.zulia.message.ZuliaBase.AssociatedDocument;
import io.zulia.message.ZuliaQuery.FetchType;
import io.zulia.rest.dto.AssociatedMetadataDTO;
import io.zulia.server.config.cluster.S3Config;
import io.zulia.server.filestorage.io.S3OutputStream;
import io.zulia.util.ZuliaUtil;
import org.bson.Document;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.ContainerCredentialsProvider;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class S3DocumentStorage implements DocumentStorage {
	private static final String TIMESTAMP = "_tstamp_";
	private static final String FILE_EXTERNAL = "_external_";
	private static final String COLLECTION = "associatedFiles.info";
	public static final String FILENAME = "filename";
	private final MongoClient client;
	private final String indexName;
	private final String dbName;
	private final String bucket;
	private final S3Client s3;
	private final String region;
	private final boolean propWait;

	public S3DocumentStorage(MongoClient mongoClient, String indexName, String dbName, boolean sharded, S3Config s3Config) {
		if (null == s3Config)
			throw new IllegalArgumentException("Must provide the s3 config section");
		if (null == s3Config.getS3BucketName())
			throw new IllegalArgumentException("Must provide the S3 bucket that is going to be used to store content");
		if (null == s3Config.getRegion())
			throw new IllegalArgumentException("Must provide the region the s3 bucket lives in.");
		this.bucket = s3Config.getS3BucketName();
		this.region = s3Config.getRegion();
		this.propWait = s3Config.isPropWait();
		this.client = mongoClient;
		this.indexName = indexName;
		this.dbName = dbName;

		AwsCredentialsProviderChain credentialsProvider = AwsCredentialsProviderChain.builder()
				.credentialsProviders(InstanceProfileCredentialsProvider.builder().build(), ContainerCredentialsProvider.builder().build(),
						EnvironmentVariableCredentialsProvider.create(), SystemPropertyCredentialsProvider.create(),
						ProfileCredentialsProvider.builder().build()).build();

		this.s3 = S3Client.builder().region(Region.of(this.region)).credentialsProvider(credentialsProvider).build();

		Thread.startVirtualThread(() -> {
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
		OutputStream os = getAssociatedDocumentOutputStream(doc.getDocumentUniqueId(), doc.getFilename(), doc.getTimestamp(),
				ZuliaUtil.byteArrayToMongoDocument(doc.getMetadata().toByteArray()));
		ByteArrayInputStream bais = new ByteArrayInputStream(doc.getDocument().toByteArray());
		try (os; bais) {
			bais.transferTo(os);
		}
	}

	@Override
	public List<AssociatedDocument> getAssociatedMetadataForUniqueId(String uniqueId, FetchType fetchType) throws Exception {
		if (FetchType.NONE.equals(fetchType))
			return Collections.emptyList();

		FindIterable<Document> found = client.getDatabase(dbName).getCollection(COLLECTION).find(Filters.eq("metadata." + DOCUMENT_UNIQUE_ID_KEY, uniqueId));
		List<AssociatedDocument> docs = new ArrayList<>();

		//Have to do it this way because the FindIterable does not implement the streams API.
		if (FetchType.META.equals(fetchType)) {
			for (Document doc : found) {
				docs.add(buildMetadataDocument(doc));
			}
		}
		else if (FetchType.FULL.equals(fetchType)) {
			for (Document doc : found) {
				docs.add(buildFullDocument(doc));
			}
		}
		else {
			return Collections.emptyList();
		}

		return docs;
	}

	@Override
	public AssociatedDocument getAssociatedDocument(String uniqueId, String filename, FetchType fetchType) throws Exception {
		String uid = String.join("-", uniqueId, filename);

		return switch (fetchType) {
			case NONE, UNRECOGNIZED -> null;
			case META ->
					buildMetadataDocument(client.getDatabase(dbName).getCollection(COLLECTION).find(Filters.eq("metadata." + FILE_UNIQUE_ID_KEY, uid)).first());
			case FULL, ALL ->
					buildFullDocument(client.getDatabase(dbName).getCollection(COLLECTION).find(Filters.eq("metadata." + FILE_UNIQUE_ID_KEY, uid)).first());
		};
	}

	@Override
	public Stream<AssociatedMetadataDTO> getAssociatedMetadataForQuery(Document query) {
		FindIterable<Document> found = client.getDatabase(dbName).getCollection(COLLECTION).find(query);

		return StreamSupport.stream(found.map(doc -> {
			Document metadata = doc.get("metadata", Document.class);
			String uniqueId = metadata.getString(DOCUMENT_UNIQUE_ID_KEY);

			String filename = doc.getString("filename");

			Date uploadDate = doc.getDate("uploadDate");

			metadata.remove(TIMESTAMP);
			metadata.remove(DOCUMENT_UNIQUE_ID_KEY);
			metadata.remove(FILE_UNIQUE_ID_KEY);

			return new AssociatedMetadataDTO(uniqueId, filename, uploadDate, metadata);
		}).spliterator(), false);

	}

	@Override
	public OutputStream getAssociatedDocumentOutputStream(String uniqueId, String fileName, long timestamp, Document metadataMap) {
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

		return new SnappyOutputStream(new S3OutputStream(s3, bucket, key, propWait));
	}

	@Override
	public InputStream getAssociatedDocumentStream(String uniqueId, String filename) throws Exception {
		FindIterable<Document> found = client.getDatabase(dbName).getCollection(COLLECTION)
				.find(Filters.eq("metadata." + FILE_UNIQUE_ID_KEY, String.join("-", uniqueId, filename)));
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
	public List<String> getAssociatedFilenames(String uniqueId) {
		FindIterable<Document> found = client.getDatabase(dbName).getCollection(COLLECTION).find(Filters.eq("metadata." + DOCUMENT_UNIQUE_ID_KEY, uniqueId));
		List<String> files = new ArrayList<>();
		found.map(doc -> doc.getString(FILENAME)).forEach(files::add);
		return files;
	}

	@Override
	public void deleteAssociatedDocument(String uniqueId, String fileName) {
		FindIterable<Document> found = client.getDatabase(dbName).getCollection(COLLECTION)
				.find(Filters.eq("metadata." + FILE_UNIQUE_ID_KEY, String.join("-", uniqueId, fileName)));
		Document doc = found.first();
		if (null != doc) {
			client.getDatabase(dbName).getCollection(COLLECTION).deleteOne(Filters.eq("_id", doc.getObjectId("_id")));
			if (!doc.getBoolean("metadata." + FILE_EXTERNAL, false)) {
				Document s3Info = doc.get("s3", Document.class);
				DeleteObjectRequest dor = DeleteObjectRequest.builder().bucket(s3Info.getString("bucket")).key(s3Info.getString("key")).build();
				s3.deleteObject(dor);
			}
		}
	}

	@Override
	public void deleteAssociatedDocuments(String uniqueId) {
		MongoCollection<Document> collection = client.getDatabase(dbName).getCollection(COLLECTION);
		FindIterable<Document> found = collection.find(Filters.eq("metadata." + DOCUMENT_UNIQUE_ID_KEY, uniqueId));
		for (Document doc : found) {
			collection.deleteOne(Filters.eq("_id", doc.getObjectId("_id")));
			if (!doc.getBoolean("metadata." + FILE_EXTERNAL, false)) {
				Document s3Info = doc.get("s3", Document.class);
				DeleteObjectRequest dor = DeleteObjectRequest.builder().bucket(s3Info.getString("bucket")).key(s3Info.getString("key")).build();
				s3.deleteObject(dor);
			}
		}
	}

	/**
	 * This translates the External document to look like an internal document stored in another S3 location that the zulia instance should have access too.
	 *
	 * @param registration
	 */
	@Override
	public void registerExternalDocument(ZuliaBase.ExternalDocument registration) {
		Document reg = ZuliaUtil.byteArrayToMongoDocument(registration.getRegistration().toByteArray());
		assert (reg.containsKey("location"));
		assert (reg.containsKey("metadata"));

		Document metadata = reg.get("metadata", Document.class);
		metadata.put(TIMESTAMP, registration.getTimestamp());
		metadata.put(DOCUMENT_UNIQUE_ID_KEY, registration.getDocumentUniqueId());
		metadata.put(FILE_UNIQUE_ID_KEY, String.join("-", registration.getDocumentUniqueId(), registration.getFilename()));
		metadata.put(FILE_EXTERNAL, true);

		Document s3Location = reg.get("location", Document.class);
		assert (s3Location.containsKey("bucket"));
		assert (s3Location.containsKey("region"));
		assert (s3Location.containsKey("key"));

		Document TOC = new Document();
		TOC.put("filename", registration.getFilename());
		TOC.put("metadata", metadata);
		TOC.put("s3", s3Location);

		client.getDatabase(dbName).getCollection(COLLECTION).insertOne(TOC);
	}

	@Override
	public void drop() {
		FindIterable<Document> found = client.getDatabase(dbName).getCollection(COLLECTION).find();
		deleteAllKeys(found);
		client.getDatabase(dbName).drop();
	}

	@Override
	public void deleteAllDocuments() {
		//Gets the list of keys only that are not stored externally.
		FindIterable<Document> found = client.getDatabase(dbName).getCollection(COLLECTION).find(Filters.ne("metadata." + FILE_EXTERNAL, true));
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
		DeleteObjectsRequest dor = DeleteObjectsRequest.builder().bucket(bucket)
				.delete(Delete.builder().objects(keyBatch.stream().map(s -> ObjectIdentifier.builder().key(s).build()).collect(Collectors.toList())).build())
				.build();
		s3.deleteObjects(dor);
	}

	private AssociatedDocument.Builder parseMongo(Document doc) {
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

		return aBuilder;
	}

	private void addFileContents(AssociatedDocument.Builder aBuilder, Document doc) throws IOException {
		Document s3Info = doc.get("s3", Document.class);
		GetObjectRequest gor = GetObjectRequest.builder().bucket(s3Info.getString("bucket")).key(s3Info.getString("key")).build();
		ResponseInputStream<GetObjectResponse> results = s3.getObject(gor);
		InputStream compression = new SnappyInputStream(results);
		try (compression) {
			aBuilder.setDocument(ByteString.readFrom(compression));
		}
	}

	private AssociatedDocument buildMetadataDocument(Document doc) {
		AssociatedDocument.Builder builder = parseMongo(doc);
		return builder.build();
	}

	private AssociatedDocument buildFullDocument(Document doc) throws IOException {
		AssociatedDocument.Builder builder = parseMongo(doc);
		addFileContents(builder, doc);
		return builder.build();
	}
}
