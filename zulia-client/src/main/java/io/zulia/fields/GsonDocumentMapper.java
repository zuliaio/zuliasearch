package io.zulia.fields;

import com.google.gson.Gson;
import io.zulia.client.result.BatchFetchResult;
import io.zulia.client.result.FetchResult;
import io.zulia.message.ZuliaQuery;
import io.zulia.util.ResultHelper;
import org.bson.Document;

import java.util.List;

public class GsonDocumentMapper<T> {

	private final Class<T> clazz;
	private final Gson gson = new Gson();

	public GsonDocumentMapper(Class<T> clazz) {
		this.clazz = clazz;
	}

	public Document toDocument(T object) {
		String json = gson.toJson(object);
		return Document.parse(json);
	}

	public T fromDocument(Document savedDocument) {
		if (savedDocument != null) {
			String json = savedDocument.toJson();
			return gson.fromJson(json, clazz);
		}
		return null;
	}

	public T fromScoredResult(ZuliaQuery.ScoredResult scoredResult) throws Exception {
		return fromDocument(ResultHelper.getDocumentFromScoredResult(scoredResult));
	}

	public List<T> fromBatchFetchResult(BatchFetchResult batchFetchResult) throws Exception {
		return batchFetchResult.getDocuments(this);
	}

	public T fromFetchResult(FetchResult fetchResult) throws Exception {
		return fetchResult.getDocument(this);
	}

}
