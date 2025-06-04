package io.zulia.server.rest.controllers;

import com.google.protobuf.ByteString;
import com.google.protobuf.ProtocolStringList;
import com.google.protobuf.util.JsonFormat;
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
import io.zulia.message.ZuliaIndex;
import io.zulia.message.ZuliaServiceOuterClass;
import io.zulia.rest.dto.IndexesResponseDTO;
import io.zulia.server.index.ZuliaIndexManager;
import io.zulia.server.util.ZuliaNodeProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static io.zulia.message.ZuliaServiceOuterClass.GetIndexesRequest;
import static io.zulia.message.ZuliaServiceOuterClass.GetIndexesResponse;

/**
 * Created by Payam Meyer on 8/7/17.
 *
 * @author pmeyer
 */
@Controller
@ApiResponses({ @ApiResponse(responseCode = "400", content = { @Content(schema = @Schema(implementation = JsonError.class)) }),
		@ApiResponse(responseCode = "404", content = { @Content(schema = @Schema(implementation = JsonError.class)) }),
		@ApiResponse(responseCode = "500", content = { @Content(schema = @Schema(implementation = JsonError.class)) }),
		@ApiResponse(responseCode = "503", content = { @Content(schema = @Schema(implementation = JsonError.class)) }) })
@ExecuteOn(TaskExecutors.BLOCKING)
public class IndexesController {
	private final static Logger LOG = LoggerFactory.getLogger(IndexesController.class);

	@Get(ZuliaRESTConstants.INDEXES_URL + "/{index}")
	@Produces(ZuliaRESTConstants.UTF8_JSON)
	public String getIndex(String index) throws Exception {
		return JsonFormat.printer().print(getIndexResponse(index));
	}

	@Get(ZuliaRESTConstants.INDEX_URL)
	@Produces(ZuliaRESTConstants.UTF8_JSON)
	public String getIndexLegacy(@QueryValue(ZuliaRESTConstants.INDEX) String index) throws Exception {
		return JsonFormat.printer().print(getIndexResponse(index));
	}

	@Get(ZuliaRESTConstants.INDEXES_URL)
	@Produces(ZuliaRESTConstants.UTF8_JSON)
	public IndexesResponseDTO getIndexes() throws Exception {

		ZuliaIndexManager indexManager = ZuliaNodeProvider.getZuliaNode().getIndexManager();

		GetIndexesResponse getIndexesResponse = indexManager.getIndexes(GetIndexesRequest.newBuilder().build());

		ProtocolStringList indexNameList = getIndexesResponse.getIndexNameList();
		List<String> sorted = new ArrayList<>(indexNameList);
		Collections.sort(sorted);

		IndexesResponseDTO indexesResponse = new IndexesResponseDTO();
		indexesResponse.setIndexes(sorted);
		return indexesResponse;
	}

	public static ZuliaServiceOuterClass.RestIndexSettingsResponse getIndexResponse(String index) throws Exception {
		ZuliaIndexManager indexManager = ZuliaNodeProvider.getZuliaNode().getIndexManager();

		ZuliaServiceOuterClass.GetIndexSettingsResponse getIndexSettingsResponse = indexManager.getIndexSettings(
				ZuliaServiceOuterClass.GetIndexSettingsRequest.newBuilder().setIndexName(index).build());

		ZuliaIndex.IndexSettings indexSettings = getIndexSettingsResponse.getIndexSettings();

		ZuliaServiceOuterClass.RestIndexSettingsResponse.Builder restIndexSettings = ZuliaServiceOuterClass.RestIndexSettingsResponse.newBuilder()
				.setIndexSettings(indexSettings);

		for (ByteString bytes : indexSettings.getWarmingSearchesList()) {
			try {
				ZuliaServiceOuterClass.QueryRequest queryRequest = ZuliaServiceOuterClass.QueryRequest.parseFrom(bytes);
				restIndexSettings.addWarmingSearch(queryRequest);
			}
			catch (Exception e) {
				LOG.warn("Failed to parse warming search: {}", e.getMessage());
			}
		}

		return restIndexSettings.build();

	}

}
