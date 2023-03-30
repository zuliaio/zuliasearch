package io.zulia.server.index.field;

import io.zulia.message.ZuliaIndex.FieldConfig.FieldType;
import io.zulia.server.field.FieldTypeUtil;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;

public class LongFieldIndexer extends NumericFieldIndexer {

	public static final LongFieldIndexer INSTANCE = new LongFieldIndexer();

	protected LongFieldIndexer() {

	}

	@Override
	protected Field createField(Number o, String indexedFieldName) {
		return new LongPoint(FieldTypeUtil.getIndexField(indexedFieldName, FieldType.NUMERIC_LONG), o.longValue());
	}

	@Override
	protected Number parseString(String value) {
		return Long.parseLong(value);
	}

}
