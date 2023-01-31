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

        Object o;
        if (storedFieldName.contains(".")) {
            o = mongoDocument;
            String[] fields = storedFieldName.split("\\.");
            for (String field : fields) {
                if (o instanceof List<?> list) {
                    List<Object> values = new ArrayList<>();
                    list.stream().filter(item -> item instanceof org.bson.Document).forEach(item -> {
                        org.bson.Document dbObj = (org.bson.Document) item;
                        Object object = dbObj.get(field);
                        if (object != null) {
                            values.add(object);
                        }
                    });
                    if (!values.isEmpty()) {
                        o = values;
                    } else {
                        o = null;
                    }
                } else if (o instanceof Document mongoDoc) {
                    o = mongoDoc.get(field);
                } else {
                    o = null;
                    break;
                }
            }
        } else {
            o = mongoDocument.get(storedFieldName);
        }

        return o;
    }

}
