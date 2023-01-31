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
import io.zulia.server.util.ZuliaNodeProvider;
import io.zulia.util.ResultHelper;
import org.bson.Document;
import org.bson.json.JsonWriterSettings;

import static io.zulia.message.ZuliaServiceOuterClass.FetchRequest;
import static io.zulia.message.ZuliaServiceOuterClass.FetchResponse;

@Controller(ZuliaConstants.FETCH_URL)
public class FetchController {

	@Get
	@Produces(MediaType.APPLICATION_JSON + ";charset=utf-8")
	public HttpResponse<?> get(@QueryValue(ZuliaConstants.ID) final String uniqueId, @QueryValue(ZuliaConstants.INDEX) final String indexName,
			@QueryValue(value = ZuliaConstants.PRETTY, defaultValue = "true") Boolean pretty) {

		ZuliaIndexManager indexManager = ZuliaNodeProvider.getZuliaNode().getIndexManager();

		FetchRequest.Builder fetchRequest = FetchRequest.newBuilder();
		fetchRequest.setIndexName(indexName);
		fetchRequest.setUniqueId(uniqueId);

		FetchResponse fetchResponse;

		try {
			fetchResponse = indexManager.fetch(fetchRequest.build());

			if (fetchResponse.hasResultDocument()) {
				Document document = ResultHelper.getDocumentFromResultDocument(fetchResponse.getResultDocument());
				if (document != null) {
					String docString;
					if (pretty) {
						docString = document.toJson(JsonWriterSettings.builder().indent(true).build());
					}
					else {
						docString = document.toJson();
					}

					if (pretty) {
						docString = JsonWriter.formatJson(docString);
					}

					return HttpResponse.ok(docString).status(ZuliaConstants.SUCCESS);
				}

			}
			return HttpResponse.ok("Failed to fetch uniqueId <" + uniqueId + "> for index <" + indexName + ">").status(ZuliaConstants.NOT_FOUND);

		}
		catch (Exception e) {
			return HttpResponse.serverError("Failed to fetch uniqueId <" + uniqueId + "> for index <" + indexName + ">: " + e.getMessage())
					.status(ZuliaConstants.INTERNAL_ERROR);
		}

	}

}