package io.zulia.server.index.field;

import io.zulia.message.ZuliaIndex.FieldConfig;
import io.zulia.server.field.FieldTypeUtil;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.TextField;

public class StringFieldIndexer extends FieldIndexer {

	private final static FieldType notStoredTextField;

	static {
		notStoredTextField = new FieldType(TextField.TYPE_NOT_STORED);
		notStoredTextField.freeze();
	}

	public static final StringFieldIndexer INSTANCE = new StringFieldIndexer();

	protected StringFieldIndexer() {

	}

	@Override
	protected void handleValue(Document d, String storedFieldName, Object value, String indexedFieldName) {

		if (value != null) {
			String val = value.toString();
			d.add((new Field(FieldTypeUtil.getIndexField(indexedFieldName, FieldConfig.FieldType.STRING), val, notStoredTextField)));
			int length = val.length();
			d.add(new IntPoint(FieldTypeUtil.getCharLengthIndexField(indexedFieldName), length));
			d.add(new SortedNumericDocValuesField(FieldTypeUtil.getCharLengthSortField(indexedFieldName), length));
		}
	}

}
