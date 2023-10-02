package io.zulia.server.rest.controllers;

import com.cedarsoftware.util.io.JsonWriter;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.JsonFormat;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.annotation.QueryValue;
import io.zulia.ZuliaRESTConstants;
import io.zulia.message.ZuliaIndex;
import io.zulia.message.ZuliaServiceOuterClass;
import io.zulia.message.ZuliaServiceOuterClass.GetIndexSettingsResponse;
import io.zulia.server.index.ZuliaIndexManager;
import io.zulia.server.util.ZuliaNodeProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Payam Meyer on 8/7/17.
 *
 * @author pmeyer
 */
@Controller(ZuliaRESTConstants.INDEX_URL)
public class IndexController {
	private final static Logger LOG = LoggerFactory.getLogger(IndexController.class);

	@Get
	@Produces(MediaType.APPLICATION_JSON + ";charset=utf-8")
	public HttpResponse<?> get(@QueryValue(ZuliaRESTConstants.INDEX) String index,
			@QueryValue(value = ZuliaRESTConstants.PRETTY, defaultValue = "true") Boolean pretty) {

		ZuliaIndexManager indexManager = ZuliaNodeProvider.getZuliaNode().getIndexManager();

		try {
			StringBuilder responseBuilder = new StringBuilder();

			GetIndexSettingsResponse getIndexSettingsResponse = indexManager.getIndexSettings(
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
					LOG.error("Failed to parse warming search: " + e.getMessage());
				}
			}

			responseBuilder.append(JsonFormat.printer().print(restIndexSettings));

			String docString = responseBuilder.toString();

			if (pretty) {
				docString = JsonWriter.formatJson(docString);
			}

			return HttpResponse.ok(docString).status(ZuliaRESTConstants.SUCCESS);

		}
		catch (Exception e) {
			return HttpResponse.serverError("Failed to get index names: " + e.getMessage()).status(ZuliaRESTConstants.INTERNAL_ERROR);
		}

	}

}
