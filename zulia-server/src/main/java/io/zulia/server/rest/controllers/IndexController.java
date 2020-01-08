package io.zulia.server.rest.controllers;

import com.cedarsoftware.util.io.JsonWriter;
import com.google.protobuf.util.JsonFormat;
import io.micronaut.context.annotation.Parameter;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Produces;
import io.zulia.ZuliaConstants;
import io.zulia.message.ZuliaServiceOuterClass;
import io.zulia.message.ZuliaServiceOuterClass.GetIndexSettingsResponse;
import io.zulia.server.index.ZuliaIndexManager;

/**
 * Created by Payam Meyer on 8/7/17.
 * @author pmeyer
 */
@Controller(ZuliaConstants.INDEX_URL)
public class IndexController {

	private ZuliaIndexManager indexManager;

	public IndexController(ZuliaIndexManager indexManager) {
		this.indexManager = indexManager;
	}

	@Get
	@Produces(MediaType.APPLICATION_JSON + ";charset=utf-8")
	public HttpResponse get(@Parameter(ZuliaConstants.INDEX) String index, @Parameter(ZuliaConstants.PRETTY) boolean pretty) {

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

			return HttpResponse.created(docString).status(ZuliaConstants.SUCCESS);

		}
		catch (Exception e) {
			return HttpResponse.created("Failed to get index names: " + e.getMessage()).status(ZuliaConstants.INTERNAL_ERROR);
		}

	}

}
