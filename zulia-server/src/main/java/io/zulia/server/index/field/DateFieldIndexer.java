package io.zulia.server.index.field;

import io.zulia.message.ZuliaIndex.FieldConfig.FieldType;
import io.zulia.server.field.FieldTypeUtil;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;

import java.util.Date;

public class DateFieldIndexer extends FieldIndexer {

	public static final DateFieldIndexer INSTANCE = new DateFieldIndexer();

	protected DateFieldIndexer() {

	}

	@Override
	protected void handleValue(Document d, String storedFieldName, Object value, String indexedFieldName) throws Exception {
		if (value != null) {
			if (value instanceof Date date) {
				d.add(createField(date, indexedFieldName));
			}
			else {
				throw new Exception(
						"Expecting collection of Date or Date for field <" + storedFieldName + "> and found <" + value.getClass().getSimpleName() + ">");
			}
		}
	}

	protected Field createField(Date o, String indexedFieldName) {
		return new LongPoint(FieldTypeUtil.getIndexField(indexedFieldName, FieldType.DATE), o.getTime());
	}

}
