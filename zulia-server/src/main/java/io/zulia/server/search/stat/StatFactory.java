package io.zulia.server.search.stat;

import io.zulia.message.ZuliaIndex;
import io.zulia.server.field.FieldTypeUtil;

import java.util.function.Supplier;

public class StatFactory {

	public static Supplier<? extends Stats<?>> getStatForFieldType(ZuliaIndex.FieldConfig.FieldType fieldType, double precision) {
		if (FieldTypeUtil.isNumericDoubleFieldType(fieldType)) {
			return () -> new DoubleDoubleStats(precision);
		}
		else if (FieldTypeUtil.isNumericFloatFieldType(fieldType)) {
			return () -> new FloatDoubleStats(precision);
		}
		else if (FieldTypeUtil.isNumericLongFieldType(fieldType)) {
			return () -> new LongLongStats(precision);
		}
		else if (FieldTypeUtil.isNumericIntFieldType(fieldType)) {
			return () -> new IntLongStats(precision);
		}
		else if (FieldTypeUtil.isBooleanFieldType(fieldType)) {
			return () -> new IntLongStats(precision);
		}
		else {
			throw new IllegalArgumentException("Can not generate stat constructor for field type <" + fieldType + ">");
		}
	}
}
