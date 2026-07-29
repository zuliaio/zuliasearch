package io.zulia.server.index.field;

import io.zulia.message.ZuliaIndex.FieldConfig.FieldType;
import io.zulia.server.field.FieldTypeUtil;
import io.zulia.util.BooleanUtil;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.IntPoint;

public class BooleanFieldIndexer extends FieldIndexer {

	public static final BooleanFieldIndexer INSTANCE = new BooleanFieldIndexer();

	protected BooleanFieldIndexer() {

	}

	@Override
	protected void handleValue(Document d, String storedFieldName, Object value, String indexedFieldName) throws Exception {
		if (value != null) {

			Boolean boolVal = BooleanUtil.parseBoolean(value);
			if (boolVal == null) {
				throw new Exception(switch (value) {
					case String s -> "String for Boolean field must be 'Yes', 'No', 'Y', 'N', '1', '0', 'True', 'False', 'T', 'F' (case insensitive) for "
							+ storedFieldName + " and found " + s;
					case Number number -> "Number for Boolean field must be 0 or 1 for " + storedFieldName + " and found " + number;
					default -> "Expecting collection of data type of Boolean, String, or Number for field " + storedFieldName + " and found " + value.getClass()
							.getSimpleName();
				});
			}
			d.add(new IntPoint(FieldTypeUtil.getIndexField(indexedFieldName, FieldType.BOOL), boolVal ? 1 : 0));
		}
	}

}
