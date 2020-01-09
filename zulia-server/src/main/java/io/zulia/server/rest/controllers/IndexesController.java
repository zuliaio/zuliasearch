package io.zulia.server.rest.controllers;

import com.cedarsoftware.util.io.JsonWriter;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Produces;
import io.zulia.ZuliaConstants;
import io.zulia.server.index.ZuliaIndexManager;
import io.zulia.server.util.ZuliaNodeProvider;
import org.bson.Document;

import static io.zulia.message.ZuliaServiceOuterClass.GetIndexesRequest;
import static io.zulia.message.ZuliaServiceOuterClass.GetIndexesResponse;

/**
 * Created by Payam Meyer on 8/7/17.
 * @author pmeyer
 */
@Controller(ZuliaConstants.INDEXES_URL)
public class IndexesController {

	@Get
	@Produces(MediaType.APPLICATION_JSON + ";charset=utf-8")
	public HttpResponse<?> get() {

		ZuliaIndexManager indexManager = ZuliaNodeProvider.getZuliaNode().getIndexManager();

		try {
			GetIndexesResponse getIndexesResponse = indexManager.getIndexes(GetIndexesRequest.newBuilder().build());

			Document mongoDocument = new org.bson.Document();
			mongoDocument.put("indexes", getIndexesResponse.getIndexNameList());
			String docString = mongoDocument.toJson();

			docString = JsonWriter.formatJson(docString);

			return HttpResponse.ok(docString).status(ZuliaConstants.SUCCESS);

		}
		catch (Exception e) {
			return HttpResponse.serverError("Failed to get index names: " + e.getMessage()).status(ZuliaConstants.INTERNAL_ERROR);
		}

	}

}
