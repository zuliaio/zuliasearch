package io.zulia.client.result;

import io.zulia.fields.GsonDocumentMapper;
import io.zulia.message.ZuliaBase.ResultDocument;
import io.zulia.util.ZuliaUtil;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static io.zulia.message.ZuliaBase.AssociatedDocument;
import static io.zulia.message.ZuliaServiceOuterClass.FetchResponse;

public class FetchResult extends Result {

	private FetchResponse fetchResponse;

	private List<AssociatedResult> associatedResults;

	public FetchResult(FetchResponse fetchResponse) {
		this.fetchResponse = fetchResponse;

		if (fetchResponse.getAssociatedDocumentCount() == 0) {
			this.associatedResults = Collections.emptyList();
		}
		else {
			this.associatedResults = new ArrayList<>();
			for (AssociatedDocument ad : fetchResponse.getAssociatedDocumentList()) {
				associatedResults.add(new AssociatedResult(ad));
			}
		}
	}

	public ResultDocument getResultDocument() {
		return fetchResponse.getResultDocument();
	}

	public String getUniqueId() {
		if (fetchResponse.hasResultDocument()) {
			return fetchResponse.getResultDocument().getUniqueId();
		}
		return null;
	}

	public String getIndexName() {
		if (fetchResponse.hasResultDocument()) {
			return fetchResponse.getResultDocument().getIndexName();
		}
		return null;
	}

	public Document getMeta() {
		if (fetchResponse.hasResultDocument()) {
			ResultDocument rd = fetchResponse.getResultDocument();
			return ZuliaUtil.byteStringToMongoDocument(rd.getMetadata());
		}
		return null;
	}

	public Document getDocument() {
		if (fetchResponse.hasResultDocument()) {
			ResultDocument rd = fetchResponse.getResultDocument();
			if (rd.getDocument() != null) {
				return ZuliaUtil.byteArrayToMongoDocument(rd.getDocument().toByteArray());
			}
		}
		return null;
	}

	public <T> T getDocument(GsonDocumentMapper<T> mapper) throws Exception {
		if (fetchResponse.hasResultDocument()) {
			Document document = getDocument();
			return mapper.fromDocument(document);
		}
		return null;
	}

	public AssociatedResult getAssociatedDocument(int index) {
		return associatedResults.get(index);
	}

	public List<AssociatedResult> getAssociatedDocuments() {
		return associatedResults;
	}

	public int getAssociatedDocumentCount() {
		return associatedResults.size();
	}

	public boolean hasResultDocument() {
		return fetchResponse.hasResultDocument() && fetchResponse.getResultDocument().getDocument() != null;
	}

	public Long getDocumentTimestamp() {
		if (fetchResponse.hasResultDocument()) {
			ResultDocument rd = fetchResponse.getResultDocument();
			return rd.getTimestamp();
		}
		return null;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("\nFetchResult: {\n");
		if (hasResultDocument()) {
			sb.append("  ResultDocument: {\n");
			sb.append("    uniqueId: ");
			sb.append(getUniqueId());
			sb.append("\n");

			sb.append("    document: ");
			sb.append(getDocument());
			sb.append("\n  }\n");

		}

		for (AssociatedResult ad : getAssociatedDocuments()) {
			sb.append("  AssociatedDocument: {\n");
			sb.append("    filename: ");
			sb.append(ad.getFilename());
			sb.append("\n  }\n");
		}

		sb.append("}\n");
		return sb.toString();
	}

}
