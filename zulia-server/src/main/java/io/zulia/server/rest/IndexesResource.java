package io.zulia.server.rest;

import com.cedarsoftware.util.io.JsonWriter;
import io.micronaut.context.annotation.Parameter;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Produces;
import io.zulia.ZuliaConstants;
import io.zulia.server.index.ZuliaIndexManager;
import org.bson.Document;

import javax.inject.Singleton;

import static io.zulia.message.ZuliaServiceOuterClass.GetIndexesRequest;
import static io.zulia.message.ZuliaServiceOuterClass.GetIndexesResponse;

/**
 * Created by Payam Meyer on 8/7/17.
 * @author pmeyer
 */
@Controller(ZuliaConstants.INDEXES_URL)
public class IndexesResource {

	@Singleton
	private ZuliaIndexManager indexManager;

	public IndexesResource(ZuliaIndexManager indexManager) {
		this.indexManager = indexManager;
	}

	@Get
	@Produces(MediaType.APPLICATION_JSON + ";charset=utf-8")
	public HttpResponse get(@Parameter(ZuliaConstants.PRETTY) boolean pretty) {

		try {
			GetIndexesResponse getIndexesResponse = indexManager.getIndexes(GetIndexesRequest.newBuilder().build());

			Document mongoDocument = new org.bson.Document();
			mongoDocument.put("indexes", getIndexesResponse.getIndexNameList());
			String docString = mongoDocument.toJson();

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
