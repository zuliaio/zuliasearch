package io.zulia.server.index.field;

import io.zulia.ZuliaConstants;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.TextField;

public class StringFieldIndexer extends FieldIndexer {

	private static FieldType notStoredTextField;

	static {
		notStoredTextField = new FieldType(TextField.TYPE_NOT_STORED);
		notStoredTextField.freeze();
	}

	public static final StringFieldIndexer INSTANCE = new StringFieldIndexer();

	protected StringFieldIndexer() {

	}

	@Override
	protected void handleValue(Document d, String storedFieldName, Object value, String indexedFieldName) throws Exception {

		if (value != null) {
			String val = value.toString();
			d.add((new Field(indexedFieldName, val, notStoredTextField)));
			int length = val.length();
			d.add(new IntPoint(ZuliaConstants.CHAR_LENGTH_PREFIX + indexedFieldName, length));
			d.add(new SortedNumericDocValuesField(ZuliaConstants.CHAR_LENGTH_PREFIX + indexedFieldName + ZuliaConstants.SORT_SUFFIX, length));
		}
	}

}
