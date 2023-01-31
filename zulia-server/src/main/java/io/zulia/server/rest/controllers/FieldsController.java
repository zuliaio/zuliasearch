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
import org.bson.Document;

import static io.zulia.message.ZuliaServiceOuterClass.GetFieldNamesRequest;
import static io.zulia.message.ZuliaServiceOuterClass.GetFieldNamesResponse;

/**
 * Created by Payam Meyer on 8/7/17.
 *
 * @author pmeyer
 */
@Controller(ZuliaConstants.FIELDS_URL)
public class FieldsController {

	@Get
	@Produces(MediaType.APPLICATION_JSON + ";charset=utf-8")
	public HttpResponse<?> get(@QueryValue(ZuliaConstants.INDEX) final String indexName,
			@QueryValue(value = ZuliaConstants.PRETTY, defaultValue = "true") Boolean pretty) {

		ZuliaIndexManager indexManager = ZuliaNodeProvider.getZuliaNode().getIndexManager();

		GetFieldNamesRequest fieldNamesRequest = GetFieldNamesRequest.newBuilder().setIndexName(indexName).build();

		GetFieldNamesResponse fieldNamesResponse;

		try {
			fieldNamesResponse = indexManager.getFieldNames(fieldNamesRequest);

			Document mongoDocument = new Document();
			mongoDocument.put("index", indexName);
			mongoDocument.put("fields", fieldNamesResponse.getFieldNameList());

			String docString = mongoDocument.toJson();

			if (pretty) {
				docString = JsonWriter.formatJson(docString);
			}

			return HttpResponse.ok(docString).status(ZuliaConstants.SUCCESS);

		}
		catch (Exception e) {
			return HttpResponse.ok("Failed to fetch fields for index <" + indexName + ">: " + e.getMessage()).status(ZuliaConstants.INTERNAL_ERROR);
		}

	}

}
