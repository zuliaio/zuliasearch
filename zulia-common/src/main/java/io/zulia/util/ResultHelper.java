package io.zulia.util;

import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

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

	public static Object getValueFromMongoDocument(org.bson.Document mongoDocument, String storedFieldName) {

		int next = storedFieldName.indexOf('.');
		if (next >= 0) {
			Object o = mongoDocument;

			int off = 0;
			while (next != -1) {
				String field = storedFieldName.substring(off, next);
				off = next + 1;
				o = getChild(o, field);
				if (o == null) {
					return null;
				}
				next = storedFieldName.indexOf('.', off);
			}
			String field = storedFieldName.substring(off);
			return getChild(o, field);
		}
		else {
			return mongoDocument.get(storedFieldName);
		}

	}

	private static Object getChild(Object o, String field) {
		if (o instanceof Document d) {
			o = d.get(field);
		}
		else if (o instanceof List<?> list) {
			List<Object> values = new ArrayList<>(list.size());
			for (Object item : list) {
				if (item instanceof Document d) {
					Object object = d.get(field);
					if (object != null) {
						values.add(object);
					}
				}
			}
			if (!values.isEmpty()) {
				o = values;
			}
			else {
				o = null;
			}
		}
		else {
			o = null;
		}
		return o;
	}

}
