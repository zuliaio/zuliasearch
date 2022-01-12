package io.zulia.server.index.field;

import io.zulia.ZuliaConstants;
import io.zulia.util.ZuliaUtil;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;

import java.util.concurrent.atomic.AtomicInteger;

public abstract class FieldIndexer {

	protected FieldIndexer() {

	}

	public void index(Document document, String storedFieldName, Object storedValue, String indexedFieldName) throws Exception {

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

		document.add(new IntPoint(ZuliaConstants.LIST_LENGTH_PREFIX + indexedFieldName, size));
		document.add(new SortedNumericDocValuesField(ZuliaConstants.LIST_LENGTH_PREFIX + indexedFieldName + ZuliaConstants.SORT_SUFFIX, size));

	}

	protected abstract void handleValue(Document d, String storedFieldName, Object value, String indexedFieldName) throws Exception;

}
