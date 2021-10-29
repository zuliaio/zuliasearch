package io.zulia.client.result;

import io.zulia.fields.GsonDocumentMapper;
import io.zulia.message.ZuliaQuery;
import io.zulia.message.ZuliaQuery.AnalysisResult;
import io.zulia.message.ZuliaQuery.FacetCount;
import io.zulia.message.ZuliaQuery.FacetGroup;
import io.zulia.message.ZuliaQuery.LastResult;
import io.zulia.message.ZuliaQuery.ScoredResult;
import io.zulia.message.ZuliaServiceOuterClass.QueryResponse;
import io.zulia.util.ResultHelper;
import io.zulia.util.ZuliaUtil;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class QueryResult extends Result {
	private QueryResponse queryResponse;

	public QueryResult(QueryResponse queryResponse) {
		this.queryResponse = queryResponse;
	}

	public long getTotalHits() {
		return queryResponse.getTotalHits();
	}

	public boolean hasResults() {
		return !queryResponse.getResultsList().isEmpty();
	}

	public List<ScoredResult> getResults() {
		return queryResponse.getResultsList();
	}

	public ScoredResult getFirstResult() {
		if (hasResults()) {
			return getResults().get(0);
		}
		return null;
	}

	public List<Document> getDocuments() {
		List<Document> documents = new ArrayList<>();
		for (ScoredResult scoredResult : queryResponse.getResultsList()) {
			Document doc = ResultHelper.getDocumentFromScoredResult(scoredResult);
			if (doc != null) {
				documents.add(doc);
			}
			else {
				throw new IllegalStateException("Cannot get results without fetch type of full");
			}
		}
		return documents;
	}

	public List<Document> getMetaDocuments() {
		List<Document> documents = new ArrayList<>();
		for (ScoredResult scoredResult : queryResponse.getResultsList()) {
			Document metadata = ZuliaUtil.byteStringToMongoDocument(scoredResult.getResultDocument().getMetadata());
			if (metadata != null) {
				documents.add(metadata);
			}
			else {
				throw new IllegalStateException("Cannot get meta document without fetch type of meta or full");
			}
		}
		return documents;
	}

	public <T> List<T> getMappedDocuments(GsonDocumentMapper<T> mapper) throws Exception {
		List<T> items = new ArrayList<>();
		for (ScoredResult scoredResult : queryResponse.getResultsList()) {
			T item = mapper.fromScoredResult(scoredResult);
			if (item != null) {
				items.add(item);
			}
			else {
				throw new IllegalStateException("Cannot get results without fetch type of full");
			}
		}
		return items;
	}

	public Document getFirstDocument() {
		if (hasResults()) {
			Document doc = ResultHelper.getDocumentFromScoredResult(getResults().get(0));
			if (doc != null) {
				return doc;
			}
			else {
				throw new IllegalStateException("Cannot get results without fetch type of full");
			}
		}
		return null;
	}

	public LastResult getLastResult() {
		return queryResponse.getLastResult();
	}

	public List<FacetGroup> getFacetGroups() {
		return queryResponse.getFacetGroupList();
	}

	public List<FacetCount> getFacetCounts(String fieldName) {
		for (FacetGroup fg : queryResponse.getFacetGroupList()) {
			if (fieldName.equals(fg.getCountRequest().getFacetField().getLabel())) {
				return fg.getFacetCountList();
			}
		}
		return null;
	}

	public List<FacetCount> getFacetCountsForPath(String fieldName, String... paths) {
		return getFacetCountsForPath(fieldName, Arrays.asList(paths));
	}

	public List<FacetCount> getFacetCountsForPath(String fieldName, List<String> paths) {
		for (FacetGroup fg : queryResponse.getFacetGroupList()) {
			ZuliaQuery.Facet facetField = fg.getCountRequest().getFacetField();
			if (fieldName.equals(facetField.getLabel()) && facetField.getPathList().equals(paths)) {
				return fg.getFacetCountList();
			}
		}
		return null;
	}

	public int getFacetGroupCount() {
		return queryResponse.getFacetGroupCount();
	}

	public List<AnalysisResult> getSummaryAnalysisResults() {
		return queryResponse.getAnalysisResultList();
	}

	@Override
	public String toString() {
		return queryResponse.toString();
	}

}
