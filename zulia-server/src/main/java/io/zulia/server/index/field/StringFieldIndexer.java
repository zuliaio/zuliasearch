package io.zulia.server.index.field;

import io.zulia.ZuliaConstants;
import org.apache.lucene.document.*;

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
			d.add(new IntPoint(ZuliaConstants.CHAR_LENGTH_PREFIX + indexedFieldName, val.length()));
		}
	}

}
