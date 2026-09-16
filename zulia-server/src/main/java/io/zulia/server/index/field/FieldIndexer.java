package io.zulia.server.index.field;

import io.zulia.server.field.FieldTypeUtil;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;

public abstract class FieldIndexer {

	protected FieldIndexer() {

	}

	public void index(Document document, String storedFieldName, StoredFieldHandler handler, String indexedFieldName) {

		handler.onAllValues(obj -> handleValue(document, storedFieldName, obj, indexedFieldName));

		//if stored value is a list or array
		int size = handler.valueCount();

		document.add(new IntPoint(FieldTypeUtil.getListLengthIndexField(indexedFieldName), size));
		document.add(new SortedNumericDocValuesField(FieldTypeUtil.getListLengthSortField(indexedFieldName), size));

	}

	protected abstract void handleValue(Document d, String storedFieldName, Object value, String indexedFieldName);

}
