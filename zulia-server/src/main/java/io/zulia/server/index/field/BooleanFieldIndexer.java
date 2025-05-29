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

			boolean boolVal;
			switch (value) {
				case Boolean val -> boolVal = val;
				case String s -> {
					int booleanInt = BooleanUtil.getStringAsBooleanInt(s);
					if (booleanInt == 1) {
						boolVal = true;
					}
					else if (booleanInt == 0) {
						boolVal = false;
					}
					else {
						throw new Exception("String for Boolean field be 'Yes', 'No', 'Y', 'N', '1', '0', 'True', 'False', 'T', 'F' (case insensitive) for "
								+ storedFieldName + " and found " + s);
					}
				}
				case Number number -> {
					int v = number.intValue();
					if (v == 0) {
						boolVal = false;
					}
					else if (v == 1) {
						boolVal = true;
					}
					else {
						throw new Exception("Number for Boolean field must be 0 or 1 for " + storedFieldName + " and found " + v );
					}
				}
				default -> throw new Exception(
						"Expecting collection of data type of Boolean, String, or Number for field " + storedFieldName + " and found " + value.getClass()
								.getSimpleName());
			}
			d.add(new IntPoint(FieldTypeUtil.getIndexField(indexedFieldName, FieldType.BOOL), boolVal ? 1 : 0));
		}
	}

}
