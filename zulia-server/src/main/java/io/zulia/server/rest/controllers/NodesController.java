package io.zulia.server.rest.controllers;

import com.cedarsoftware.util.io.JsonWriter;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.annotation.QueryValue;
import io.zulia.ZuliaRESTConstants;
import io.zulia.message.ZuliaServiceOuterClass;
import io.zulia.message.ZuliaServiceOuterClass.GetIndexSettingsResponse;
import io.zulia.server.index.ZuliaIndexManager;
import io.zulia.server.node.ZuliaNode;
import io.zulia.server.util.ZuliaNodeProvider;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import static io.zulia.message.ZuliaBase.Node;
import static io.zulia.message.ZuliaIndex.IndexShardMapping;
import static io.zulia.message.ZuliaIndex.ShardMapping;
import static io.zulia.message.ZuliaServiceOuterClass.GetNodesRequest;
import static io.zulia.message.ZuliaServiceOuterClass.GetNodesResponse;

/**
 * Created by Payam Meyer on 8/7/17.
 *
 * @author pmeyer
 */
@Controller(ZuliaRESTConstants.NODES_URL)
public class NodesController {

	@Get
	@Produces(MediaType.APPLICATION_JSON + ";charset=utf-8")
	public HttpResponse<?> get(@QueryValue(value = ZuliaRESTConstants.PRETTY, defaultValue = "true") Boolean pretty,
			@QueryValue(value = ZuliaRESTConstants.ACTIVE, defaultValue = "false") Boolean active) {

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
				for (IndexShardMapping indexShardMapping : getNodesResponse.getIndexShardMappingList()) {

					TreeSet<Integer> primaryShards = new TreeSet<>();
					TreeSet<Integer> replicaShards = new TreeSet<>();
					for (ShardMapping shardMapping : indexShardMapping.getShardMappingList()) {
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
					shards.put("name", indexShardMapping.getIndexName());

					int indexWeight = -1;
					GetIndexSettingsResponse indexSettings = indexManager.getIndexSettings(
							ZuliaServiceOuterClass.GetIndexSettingsRequest.newBuilder().setIndexName(indexShardMapping.getIndexName()).build());
					if (indexSettings != null) { //paranoid
						indexWeight = indexSettings.getIndexSettings().getIndexWeight();
					}

					shards.put("indexWeight", indexWeight);
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

			return HttpResponse.ok(docString).status(ZuliaRESTConstants.SUCCESS);

		}
		catch (Exception e) {
			return HttpResponse.serverError("Failed to get cluster membership: " + e.getMessage()).status(ZuliaRESTConstants.INTERNAL_ERROR);
		}

	}

}
