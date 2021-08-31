package io.zulia.server.config.cluster;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.Updates;
import io.zulia.message.ZuliaBase.Node;
import io.zulia.server.config.NodeService;
import jakarta.inject.Singleton;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@Singleton
public class MongoNodeService implements NodeService {

	private static final String NODES = "nodes";
	private static final String SERVER_ADDRESS = "serverAddress";
	private static final String SERVICE_PORT = "servicePort";
	private static final String REST_PORT = "restPort";
	private static final String HEARTBEAT = "heartbeat";

	private MongoClient mongoClient;
	private final String clusterName;

	public MongoNodeService(MongoClient mongoClient, String clusterName) {
		this.mongoClient = mongoClient;
		this.clusterName = clusterName;

		MongoCollection<Document> collection = getCollection();
		collection.createIndex(new Document(SERVER_ADDRESS, 1).append(SERVICE_PORT, 1), new IndexOptions().background(true));

	}

	private MongoCollection<Document> getCollection() {
		return mongoClient.getDatabase(clusterName).getCollection(NODES);
	}

	@Override
	public Collection<Node> getNodes() {

		List<Node> nodes = new ArrayList<>();
		for (Document d : getCollection().find()) {
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

		getCollection().replaceOne(query, nodeToDocument(node), new ReplaceOptions().upsert(true));

	}

	@Override
	public void updateHeartbeat(String serverAddress, int servicePort) {
		Document query = new Document(SERVER_ADDRESS, serverAddress).append(SERVICE_PORT, servicePort);

		Bson update = Updates.currentDate(HEARTBEAT);

		getCollection().updateOne(query, update);

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

	private Document nodeToDocument(Node node) {
		return new Document(SERVER_ADDRESS, node.getServerAddress()).append(SERVICE_PORT, node.getServicePort()).append(SERVICE_PORT, node.getServicePort())
				.append(REST_PORT, node.getRestPort());
	}

	private Node documentToNode(Document d) {
		if (d != null) {
			return Node.newBuilder().setServerAddress(d.getString(SERVER_ADDRESS)).setServicePort(d.getInteger(SERVICE_PORT))
					.setRestPort(d.getInteger(REST_PORT)).setHeartbeat(d.getDate(HEARTBEAT) != null ? d.getDate(HEARTBEAT).getTime() : 0).build();
		}
		return null;
	}
}
