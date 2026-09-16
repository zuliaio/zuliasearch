package io.zulia.server.index.field;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

public abstract class NumericFieldIndexer extends FieldIndexer {

	protected NumericFieldIndexer() {

	}

	@Override
	protected void handleValue(Document d, String storedFieldName, Object value, String indexedFieldName) {
		// the resolver already parsed numeric strings and rejected anything that is not a number
		if (value instanceof Number number) {
			d.add(createField(number, indexedFieldName));
		}
	}

	protected abstract Field createField(Number o, String indexedFieldName);

}
