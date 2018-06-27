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

import static io.zulia.message.ZuliaServiceOuterClass.GetFieldNamesRequest;
import static io.zulia.message.ZuliaServiceOuterClass.GetFieldNamesResponse;

/**
 * Created by Payam Meyer on 8/7/17.
 * @author pmeyer
 */
@Controller(ZuliaConstants.FIELDS_URL)
public class FieldsResource {

	@Singleton
	private ZuliaIndexManager indexManager;

	public FieldsResource(ZuliaIndexManager indexManager) {
		this.indexManager = indexManager;
	}

	@Get
	@Produces(MediaType.APPLICATION_JSON + ";charset=utf-8")
	public HttpResponse get(@Parameter(ZuliaConstants.INDEX) final String indexName, @Parameter(ZuliaConstants.PRETTY) boolean pretty) {

		if (indexName != null) {

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

				return HttpResponse.created(docString).status(ZuliaConstants.SUCCESS);

			}
			catch (Exception e) {
				return HttpResponse.created("Failed to fetch fields for index <" + indexName + ">: " + e.getMessage()).status(ZuliaConstants.INTERNAL_ERROR);
			}
		}
		else {
			return HttpResponse.created("No index defined").status(ZuliaConstants.INTERNAL_ERROR);
		}

	}

}
