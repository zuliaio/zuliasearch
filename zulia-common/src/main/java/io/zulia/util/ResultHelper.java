package io.zulia.util;

import io.zulia.util.document.DocumentHelper;
import org.bson.Document;

import static io.zulia.message.ZuliaBase.ResultDocument;
import static io.zulia.message.ZuliaBase.ResultDocumentOrBuilder;
import static io.zulia.message.ZuliaQuery.ScoredResult;

/**
 * Created by Matt Davis on 2/1/16.
 */
public class ResultHelper {

	public static Document getDocumentFromScoredResult(ScoredResult scoredResult) {
		if (scoredResult.hasResultDocument()) {
			ResultDocument rd = scoredResult.getResultDocument();
			return getDocumentFromResultDocument(rd);
		}
		return null;
	}

	public static Document getDocumentFromResultDocument(ResultDocumentOrBuilder rd) {
		if (rd.getDocument() != null) {
			return ZuliaUtil.byteArrayToMongoDocument(rd.getDocument().toByteArray());
		}
		return null;
	}

	@Deprecated
	public static Object getValueFromMongoDocument(org.bson.Document mongoDocument, String storedFieldName) {
		return DocumentHelper.getValueFromMongoDocument(mongoDocument, storedFieldName);
	}

}
