package io.zulia.server.index.field;

import io.zulia.server.field.FieldTypeUtil;
import io.zulia.util.ZuliaUtil;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;

import java.util.concurrent.atomic.AtomicInteger;

public abstract class FieldIndexer {

	protected FieldIndexer() {

	}

	public void index(Document document, String storedFieldName, Object storedValue, String indexedFieldName) {

		AtomicInteger listSize = new AtomicInteger();
		ZuliaUtil.handleLists(storedValue, obj -> {
			try {
				handleValue(document, storedFieldName, obj, indexedFieldName);
			}
			catch (Exception e) {
				throw new RuntimeException(e);
			}
		}, listSize);

		//if stored value is a list or array
		int size = listSize.get();

		document.add(new IntPoint(FieldTypeUtil.getListLengthIndexField(indexedFieldName), size));
		document.add(new SortedNumericDocValuesField(FieldTypeUtil.getListLengthSortField(indexedFieldName), size));

	}

	protected abstract void handleValue(Document d, String storedFieldName, Object value, String indexedFieldName) throws Exception;

}
