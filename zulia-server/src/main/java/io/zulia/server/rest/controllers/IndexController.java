package io.zulia.server.rest.controllers;

import com.cedarsoftware.util.io.JsonWriter;
import com.google.protobuf.util.JsonFormat;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.annotation.QueryValue;
import io.zulia.ZuliaConstants;
import io.zulia.message.ZuliaServiceOuterClass;
import io.zulia.message.ZuliaServiceOuterClass.GetIndexSettingsResponse;
import io.zulia.server.index.ZuliaIndexManager;
import io.zulia.server.util.ZuliaNodeProvider;

/**
 * Created by Payam Meyer on 8/7/17.
 * @author pmeyer
 */
@Controller(ZuliaConstants.INDEX_URL)
public class IndexController {

	@Get
	@Produces(MediaType.APPLICATION_JSON + ";charset=utf-8")
	public HttpResponse<?> get(@QueryValue(ZuliaConstants.INDEX) String index,
			@QueryValue(value = ZuliaConstants.PRETTY, defaultValue = "true") Boolean pretty) {

		ZuliaIndexManager indexManager = ZuliaNodeProvider.getZuliaNode().getIndexManager();

		try {
			StringBuilder responseBuilder = new StringBuilder();

			GetIndexSettingsResponse getIndexSettingsResponse = indexManager
					.getIndexSettings(ZuliaServiceOuterClass.GetIndexSettingsRequest.newBuilder().setIndexName(index).build());

			responseBuilder.append("{");
			responseBuilder.append("\"indexSettings\": ");
			JsonFormat.Printer printer = JsonFormat.printer();
			responseBuilder.append(printer.print(getIndexSettingsResponse.getIndexSettings()));
			responseBuilder.append("}");

			String docString = responseBuilder.toString();

			if (pretty) {
				docString = JsonWriter.formatJson(docString);
			}

			return HttpResponse.ok(docString).status(ZuliaConstants.SUCCESS);

		}
		catch (Exception e) {
			return HttpResponse.serverError("Failed to get index names: " + e.getMessage()).status(ZuliaConstants.INTERNAL_ERROR);
		}

	}

}
