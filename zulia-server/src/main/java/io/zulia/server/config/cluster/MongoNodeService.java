package io.zulia.server.config.cluster;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.Updates;
import io.zulia.message.ZuliaBase.Node;
import io.zulia.server.config.NodeService;
import io.zulia.util.ZuliaVersion;
import io.zulia.util.document.DocumentHelper;
import jakarta.inject.Singleton;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Singleton
public class MongoNodeService implements NodeService {

	private static final String NODES = "nodes";
	private static final String SERVER_ADDRESS = "serverAddress";
	private static final String SERVICE_PORT = "servicePort";
	private static final String REST_PORT = "restPort";
	private static final String HEARTBEAT = "heartbeat";
	private static final String CLUSTER_TIME_PROBE = "clusterTimeProbe";
	private static final String VERSION = "version";

	private final MongoClient mongoClient;
	private final String clusterName;

	public MongoNodeService(MongoClient mongoClient, String clusterName) {
		this.mongoClient = mongoClient;
		this.clusterName = clusterName;

		MongoCollection<Document> collection = getCollection();
		collection.createIndex(new Document(SERVER_ADDRESS, 1).append(SERVICE_PORT, 1), new IndexOptions().unique(true).background(true));

	}

	private MongoCollection<Document> getCollection() {
		return mongoClient.getDatabase(clusterName).getCollection(NODES);
	}

	private MongoCollection<Document> getCollectionWithTimeout() {
		return getCollection().withTimeout(5, TimeUnit.SECONDS);
	}

	@Override
	public Collection<Node> getNodes() {

		List<Node> nodes = new ArrayList<>();
		for (Document d : getCollectionWithTimeout().find()) {
			Node node = documentToNode(d);
			nodes.add(node);

		}

		return nodes;
	}

	@Override
	public Node getNode(String serverAddress, int servicePort) {

		Document query = new Document(SERVER_ADDRESS, serverAddress).append(SERVICE_PORT, servicePort);
		Document d = getCollection().find(query).first();

		return documentToNode(d);

	}

	@Override
	public void addNode(Node node) {

		Document query = new Document(SERVER_ADDRESS, node.getServerAddress()).append(SERVICE_PORT, node.getServicePort());

		// only set the registration fields: replacing the whole document erased a live node's heartbeat,
		// and every peer then expelled the healthy node through the clean-shutdown path within a second
		Bson update = Updates.combine(Updates.set(SERVER_ADDRESS, node.getServerAddress()), Updates.set(SERVICE_PORT, node.getServicePort()),
				Updates.set(REST_PORT, node.getRestPort()), Updates.set(VERSION, node.getVersion()));
		getCollection().updateOne(query, update, new UpdateOptions().upsert(true));

	}

	@Override
	public void updateHeartbeat(String serverAddress, int servicePort) {
		Document query = new Document(SERVER_ADDRESS, serverAddress).append(SERVICE_PORT, servicePort);

		Bson update = Updates.currentDate(HEARTBEAT);

		getCollectionWithTimeout().updateOne(query, update);

	}

	@Override
	public long getClusterTime(String serverAddress, int servicePort) {
		Document query = new Document(SERVER_ADDRESS, serverAddress).append(SERVICE_PORT, servicePort);
		Document updated = getCollectionWithTimeout().findOneAndUpdate(query, Updates.currentDate(CLUSTER_TIME_PROBE),
				new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));
		Date probe = updated != null ? updated.getDate(CLUSTER_TIME_PROBE) : null;
		// the document is missing only when this node was never registered, fall back to the local clock
		return probe != null ? probe.getTime() : System.currentTimeMillis();
	}

	@Override
	public void updateVersion(String serverAddress, int servicePort, String version) {
		Document query = new Document(SERVER_ADDRESS, serverAddress).append(SERVICE_PORT, servicePort);
		getCollection().updateOne(query, Updates.set(VERSION, version));
	}

	@Override
	public void removeHeartbeat(String serverAddress, int servicePort) {
		Document query = new Document(SERVER_ADDRESS, serverAddress).append(SERVICE_PORT, servicePort);

		Bson update = Updates.unset(HEARTBEAT);

		getCollection().updateOne(query, update);

	}

	@Override
	public void removeNode(String serverAddress, int servicePort) {

		getCollection().deleteOne(new Document(SERVER_ADDRESS, serverAddress).append(SERVICE_PORT, servicePort));
	}

	private Node documentToNode(Document d) {
		if (d != null) {
			String version = d.getString(VERSION);
			if (version == null) {
				version = "";
			}
			Date heartbeatDate = d.getDate(HEARTBEAT);
			return Node.newBuilder().setServerAddress(d.getString(SERVER_ADDRESS)).setServicePort(DocumentHelper.getAsInt(d, SERVICE_PORT, 0))
					.setRestPort(DocumentHelper.getAsInt(d, REST_PORT, 0)).setHeartbeat(heartbeatDate != null ? heartbeatDate.getTime() : 0)
					.setVersion(version).build();
		}
		return null;
	}
}
