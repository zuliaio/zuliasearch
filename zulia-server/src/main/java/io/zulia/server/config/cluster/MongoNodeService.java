package io.zulia.server.config.cluster;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.UpdateOptions;
import io.zulia.message.ZuliaBase.Node;
import io.zulia.server.config.NodeService;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class MongoNodeService implements NodeService {

	private static final String SERVER_ADDRESS = "serverAddress";
	private static final String HAZELCAST_PORT = "hazelcastPort";
	private static final String NODES = "nodes";
	private static final String SERVICE_PORT = "servicePort";
	private static final String REST_PORT = "restPort";

	private final MongoClient mongoClient;
	private final String clusterName;

	public MongoNodeService(MongoClient mongoClient, String clusterName) {
		this.mongoClient = mongoClient;
		this.clusterName = clusterName;

		MongoCollection<Document> collection = getCollection();
		collection.createIndex(new Document(SERVER_ADDRESS, 1).append(HAZELCAST_PORT, 1));

	}

	private MongoCollection<Document> getCollection() {
		return mongoClient.getDatabase(clusterName).getCollection(NODES);
	}

	@Override
	public Collection<Node> getNodes() {

		List<Node> nodes = new ArrayList<>();
		for (Document d : getCollection().find()) {
			Node node = Node.newBuilder().setServerAddress(d.getString(SERVER_ADDRESS)).setHazelcastPort(d.getInteger(HAZELCAST_PORT))
					.setServicePort(d.getInteger(SERVICE_PORT)).setRestPort(d.getInteger(REST_PORT)).build();
			nodes.add(node);

		}

		return nodes;
	}

	@Override
	public Node getNode(String serverAddress, int hazelcastPort) {
		List<Node> nodes = new ArrayList<>();
		Document query = new Document(SERVER_ADDRESS, serverAddress).append(HAZELCAST_PORT, hazelcastPort);
		Document d = getCollection().find(query).first();

		if (d != null) {
			return Node.newBuilder().setServerAddress(d.getString(SERVER_ADDRESS)).setHazelcastPort(d.getInteger(HAZELCAST_PORT))
					.setServicePort(d.getInteger(SERVICE_PORT)).setRestPort(d.getInteger(REST_PORT)).build();
		}

		return null;
	}

	@Override
	public void addNode(Node node) {

		Document query = new Document(SERVER_ADDRESS, node.getServerAddress()).append(HAZELCAST_PORT, node.getHazelcastPort());

		Document document = new Document(SERVER_ADDRESS, node.getServerAddress()).append(HAZELCAST_PORT, node.getHazelcastPort())
				.append(SERVICE_PORT, node.getServicePort()).append(REST_PORT, node.getRestPort());

		getCollection().replaceOne(query, document, new UpdateOptions().upsert(true));

	}

	@Override
	public void removeNode(String serverAddress, int hazelcastPort) {

		getCollection().deleteOne(new Document(SERVER_ADDRESS, serverAddress).append(HAZELCAST_PORT, hazelcastPort));
	}
}
