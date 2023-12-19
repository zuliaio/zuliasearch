package io.zulia.server.rest.controllers;

import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.annotation.QueryValue;
import io.micronaut.http.hateoas.JsonError;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.zulia.ZuliaRESTConstants;
import io.zulia.message.ZuliaBase;
import io.zulia.message.ZuliaServiceOuterClass;
import io.zulia.message.ZuliaServiceOuterClass.GetIndexSettingsResponse;
import io.zulia.rest.dto.IndexMappingDTO;
import io.zulia.rest.dto.NodeDTO;
import io.zulia.rest.dto.NodesResponseDTO;
import io.zulia.server.index.ZuliaIndexManager;
import io.zulia.server.node.ZuliaNode;
import io.zulia.server.util.ZuliaNodeProvider;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import static io.zulia.message.ZuliaIndex.IndexShardMapping;
import static io.zulia.message.ZuliaIndex.ShardMapping;
import static io.zulia.message.ZuliaServiceOuterClass.GetNodesRequest;
import static io.zulia.message.ZuliaServiceOuterClass.GetNodesResponse;

/**
 * Created by Payam Meyer on 8/7/17.
 *
 * @author pmeyer
 */
@Controller()
@ApiResponses({ @ApiResponse(responseCode = "400", content = { @Content(schema = @Schema(implementation = JsonError.class)) }),
		@ApiResponse(responseCode = "404", content = { @Content(schema = @Schema(implementation = JsonError.class)) }),
		@ApiResponse(responseCode = "500", content = { @Content(schema = @Schema(implementation = JsonError.class)) }),
		@ApiResponse(responseCode = "503", content = { @Content(schema = @Schema(implementation = JsonError.class)) }) })
public class NodesController {

	@ExecuteOn(TaskExecutors.BLOCKING)
	@Get(ZuliaRESTConstants.NODES_URL)
	@Produces(ZuliaRESTConstants.UTF8_JSON)
	public NodesResponseDTO getNodes(@QueryValue(value = ZuliaRESTConstants.ACTIVE, defaultValue = "false") Boolean active) throws Exception {

		ZuliaIndexManager indexManager = ZuliaNodeProvider.getZuliaNode().getIndexManager();

		GetNodesResponse getNodesResponse = indexManager.getNodes(GetNodesRequest.newBuilder().setActiveOnly(active).build());

		NodesResponseDTO nodesResponse = new NodesResponseDTO();

		List<NodeDTO> members = new ArrayList<>();
		for (ZuliaBase.Node node : getNodesResponse.getNodeList()) {
			NodeDTO memberObj = new NodeDTO();
			memberObj.setServerAddress(node.getServerAddress());
			memberObj.setServicePort(node.getServicePort());
			memberObj.setRestPort(node.getRestPort());
			memberObj.setHeartbeat(node.getHeartbeat());

			List<IndexMappingDTO> indexMappingList = new ArrayList<>();
			for (IndexShardMapping indexShardMapping : getNodesResponse.getIndexShardMappingList()) {

				TreeSet<Integer> primaryShards = new TreeSet<>();
				TreeSet<Integer> replicaShards = new TreeSet<>();
				for (ShardMapping shardMapping : indexShardMapping.getShardMappingList()) {
					if (ZuliaNode.isEqual(shardMapping.getPrimaryNode(), node)) {
						primaryShards.add(shardMapping.getShardNumber());
					}
					for (ZuliaBase.Node replica : shardMapping.getReplicaNodeList()) {
						if (ZuliaNode.isEqual(replica, node)) {
							replicaShards.add(shardMapping.getShardNumber());
						}
					}

				}

				IndexMappingDTO indexMappingDTO = new IndexMappingDTO();
				indexMappingDTO.setName(indexShardMapping.getIndexName());

				int indexWeight = -1;
				GetIndexSettingsResponse indexSettings = indexManager.getIndexSettings(
						ZuliaServiceOuterClass.GetIndexSettingsRequest.newBuilder().setIndexName(indexShardMapping.getIndexName()).build());
				if (indexSettings != null) { //paranoid
					indexWeight = indexSettings.getIndexSettings().getIndexWeight();
				}

				indexMappingDTO.setIndexWeight(indexWeight);
				indexMappingDTO.setPrimary(primaryShards);
				indexMappingDTO.setReplica(replicaShards);

				indexMappingList.add(indexMappingDTO);
			}
			memberObj.setIndexMappings(indexMappingList);

			members.add(memberObj);

		}

		nodesResponse.setMembers(members);
		return nodesResponse;
	}

}
