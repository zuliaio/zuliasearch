package io.zulia.server.rest.controllers;

import com.cedarsoftware.util.io.JsonWriter;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.annotation.QueryValue;
import io.zulia.ZuliaConstants;
import io.zulia.server.index.ZuliaIndexManager;
import io.zulia.server.node.ZuliaNode;
import io.zulia.server.util.ZuliaNodeProvider;
import org.bson.Document;

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
@Controller(ZuliaConstants.NODES_URL)
public class NodesController {

	@Get
	@Produces(MediaType.APPLICATION_JSON + ";charset=utf-8")
	public HttpResponse<?> get(@QueryValue(value = ZuliaConstants.PRETTY, defaultValue = "true") Boolean pretty,
			@QueryValue(value = ZuliaConstants.ACTIVE, defaultValue = "false") Boolean active) {

		ZuliaIndexManager indexManager = ZuliaNodeProvider.getZuliaNode().getIndexManager();

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

				List<Document> indexMappingList = new ArrayList<>();
				for (IndexMapping indexMapping : getNodesResponse.getIndexMappingList()) {

					TreeSet<Integer> primaryShards = new TreeSet<>();
					TreeSet<Integer> replicaShards = new TreeSet<>();
					for (ShardMapping shardMapping : indexMapping.getShardMappingList()) {
						if (ZuliaNode.isEqual(shardMapping.getPrimaryNode(), node)) {
							primaryShards.add(shardMapping.getShardNumber());
						}
						for (Node replica : shardMapping.getReplicaNodeList()) {
							if (ZuliaNode.isEqual(replica, node)) {
								replicaShards.add(shardMapping.getShardNumber());
							}
						}

					}

					Document shards = new Document();
					shards.put("name", indexMapping.getIndexName());
					shards.put("size", indexMapping.getSerializedSize());
					shards.put("primary", primaryShards);
					shards.put("replica", replicaShards);

					indexMappingList.add(shards);
				}
				memberObj.put("indexMappings", indexMappingList);

				memberObjList.add(memberObj);

			}

			mongoDocument.put("members", memberObjList);

			String docString = mongoDocument.toJson();

			if (pretty) {
				docString = JsonWriter.formatJson(docString);
			}

			return HttpResponse.ok(docString).status(ZuliaConstants.SUCCESS);

		}
		catch (Exception e) {
			return HttpResponse.serverError("Failed to get cluster membership: " + e.getMessage()).status(ZuliaConstants.INTERNAL_ERROR);
		}

	}

}
