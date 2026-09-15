package io.zulia.server.index.field;

import io.zulia.message.ZuliaIndex.FieldConfig.FieldType;
import io.zulia.server.field.FieldTypeUtil;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.IntPoint;

public class BooleanFieldIndexer extends FieldIndexer {

	public static final BooleanFieldIndexer INSTANCE = new BooleanFieldIndexer();

	protected BooleanFieldIndexer() {

	}

	@Override
	protected void handleValue(Document d, String storedFieldName, Object value, String indexedFieldName) {
		// the resolver already parsed these and rejected anything that is not a boolean
		if (value instanceof Boolean boolVal) {
			d.add(new IntPoint(FieldTypeUtil.getIndexField(indexedFieldName, FieldType.BOOL), boolVal ? 1 : 0));
		}
	}

}
