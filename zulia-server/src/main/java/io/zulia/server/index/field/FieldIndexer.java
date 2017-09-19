package io.zulia.server.index.field;

import io.zulia.util.ZuliaUtil;
import org.apache.lucene.document.Document;

public abstract class FieldIndexer {

	protected FieldIndexer() {

	}

	public void index(Document document, String storedFieldName, Object storedValue, String indexedFieldName) throws Exception {

		ZuliaUtil.handleLists(storedValue, obj -> {
			try {
				handleValue(document, storedFieldName, obj, indexedFieldName);
			}
			catch (Exception e) {
				throw new RuntimeException(e);
			}
		});

	}

	protected abstract void handleValue(Document d, String storedFieldName, Object value, String indexedFieldName) throws Exception;

}
