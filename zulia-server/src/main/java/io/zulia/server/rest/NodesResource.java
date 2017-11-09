package io.zulia.server.rest;

import com.cedarsoftware.util.io.JsonWriter;
import io.zulia.ZuliaConstants;
import io.zulia.server.index.ZuliaIndexManager;
import io.zulia.server.node.ZuliaNode;
import org.bson.Document;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import static io.zulia.message.ZuliaBase.Node;
import static io.zulia.message.ZuliaIndex.IndexMapping;
import static io.zulia.message.ZuliaIndex.ShardMapping;
import static io.zulia.message.ZuliaServiceOuterClass.GetNodesRequest;
import static io.zulia.message.ZuliaServiceOuterClass.GetNodesResponse;

/**
 * Created by Payam Meyer on 8/7/17.
 * @author pmeyer
 */
@Path(ZuliaConstants.NODES_URL)
public class NodesResource {

	private ZuliaIndexManager indexManager;

	public NodesResource(ZuliaIndexManager indexManager) {
		this.indexManager = indexManager;
	}

	@GET
	@Produces({ MediaType.APPLICATION_JSON + ";charset=utf-8" })
	public Response get(@Context Response response, @QueryParam(ZuliaConstants.PRETTY) boolean pretty, @QueryParam(ZuliaConstants.ACTIVE) boolean active) {

		try {
			GetNodesResponse getNodesResponse = indexManager.getNodes(GetNodesRequest.newBuilder().setActiveOnly(active).build());

			org.bson.Document mongoDocument = new org.bson.Document();

			List<Document> memberObjList = new ArrayList<>();
			for (Node node : getNodesResponse.getNodeList()) {
				Document memberObj = new Document();
				memberObj.put("serverAddress", node.getServerAddress());
				memberObj.put("servicePort", node.getServicePort());
				memberObj.put("restPort", node.getRestPort());
				memberObj.put("heartbeat", node.getHeartbeat());

				Document indexMappingObj = new Document();
				for (IndexMapping indexMapping : getNodesResponse.getIndexMappingList()) {

					TreeSet<Integer> primaryShards = new TreeSet<>();
					TreeSet<Integer> replicaShards = new TreeSet<>();
					for (ShardMapping shardMapping : indexMapping.getShardMappingList()) {
						if (ZuliaNode.isEqual(shardMapping.getPrimayNode(), node)) {
							primaryShards.add(shardMapping.getShardNumber());
						}
						for (Node replica : shardMapping.getReplicaNodeList()) {
							if (ZuliaNode.isEqual(replica, node)) {
								replicaShards.add(shardMapping.getShardNumber());
							}
						}

					}

					Document shards = new Document();
					indexMappingObj.put(indexMapping.getIndexName(), shards);
					shards.put("primary", primaryShards);
					shards.put("replica", replicaShards);

				}
				memberObj.put("indexMapping", indexMappingObj);

				memberObjList.add(memberObj);

			}

			mongoDocument.put("members", memberObjList);

			String docString = mongoDocument.toJson();

			if (pretty) {
				docString = JsonWriter.formatJson(docString);
			}

			return Response.status(ZuliaConstants.SUCCESS).entity(docString).build();

		}
		catch (Exception e) {
			return Response.status(ZuliaConstants.INTERNAL_ERROR).entity("Failed to get cluster membership: " + e.getMessage()).build();
		}

	}

}
