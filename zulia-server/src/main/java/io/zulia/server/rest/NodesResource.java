package io.zulia.server.rest;

import com.cedarsoftware.util.io.JsonWriter;
import io.zulia.ZuliaConstants;
import io.zulia.server.index.ZuliaIndexManager;
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
	public Response get(@Context Response response, @QueryParam(ZuliaConstants.PRETTY) boolean pretty) {

		try {
			GetNodesResponse getNodesResponse = indexManager.getNodes(GetNodesRequest.newBuilder().build());

			org.bson.Document mongoDocument = new org.bson.Document();

			List<Document> memberObjList = new ArrayList<>();
			for (Node node : getNodesResponse.getNodeList()) {
				Document memberObj = new Document();
				memberObj.put("serverAddress", node.getServerAddress());
				memberObj.put("servicePort", node.getServicePort());
				memberObj.put("restPort", node.getRestPort());

				Document indexMappingObj = new Document();
				for (IndexMapping indexMapping : getNodesResponse.getIndexMappingList()) {

					TreeSet<Integer> shards = new TreeSet<>();
					for (ShardMapping shardMapping : indexMapping.getShardMappingList()) {
						if (shardMapping.getPrimayNode().equals(node)) {
							shards.add(shardMapping.getShardNumber());
						}
					}

					indexMappingObj.put(indexMapping.getIndexName(), shards);
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
