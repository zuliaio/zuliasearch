package io.zulia.server.rest.controllers;

import com.cedarsoftware.util.io.JsonWriter;
import com.google.protobuf.ProtocolStringList;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Produces;
import io.zulia.ZuliaConstants;
import io.zulia.server.index.ZuliaIndexManager;
import io.zulia.server.util.ZuliaNodeProvider;
import org.bson.Document;

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
@Controller(ZuliaConstants.INDEXES_URL)
public class IndexesController {

	@Get
	@Produces(MediaType.APPLICATION_JSON + ";charset=utf-8")
	public HttpResponse<?> get() {

		ZuliaIndexManager indexManager = ZuliaNodeProvider.getZuliaNode().getIndexManager();

		try {
			GetIndexesResponse getIndexesResponse = indexManager.getIndexes(GetIndexesRequest.newBuilder().build());

			Document mongoDocument = new org.bson.Document();
			ProtocolStringList indexNameList = getIndexesResponse.getIndexNameList();
			List<String> sorted = new ArrayList<>(indexNameList);
			Collections.sort(sorted);
			mongoDocument.put("indexes", sorted);
			String docString = mongoDocument.toJson();

			docString = JsonWriter.formatJson(docString);

			return HttpResponse.ok(docString).status(ZuliaConstants.SUCCESS);

		}
		catch (Exception e) {
			return HttpResponse.serverError("Failed to get index names: " + e.getMessage()).status(ZuliaConstants.INTERNAL_ERROR);
		}

	}

}
