package io.zulia.server.field;

import io.zulia.message.ZuliaIndex.DefaultValue;
import io.zulia.message.ZuliaIndex.FieldConfig;
import io.zulia.util.DefaultValueUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A field config's default resolved once at settings load, so nothing is converted per document.
 */
public record FieldDefault(Object value, DefaultValue.Fill fill) {

	private static final Logger LOG = LoggerFactory.getLogger(FieldDefault.class);

	/**
	 * The resolved default, or null when the field config has none or carries one that never passed validation.
	 */
	public static FieldDefault from(FieldConfig fieldConfig) {
		if (!fieldConfig.hasDefaultValue()) {
			return null;
		}
		DefaultValue defaultValue = fieldConfig.getDefaultValue();
		try {
			DefaultValueUtil.validate(fieldConfig.getStoredFieldName(), fieldConfig.getFieldType(), defaultValue);
			return new FieldDefault(DefaultValueUtil.resolve(defaultValue), defaultValue.getFill());
		}
		catch (IllegalArgumentException e) {
			// a default that never passed validation must not keep the whole index from loading, the same way a corrupt
			// warming search does not. The field indexes as if it had no default until the settings are stored again
			LOG.error("Ignoring the default on field <{}>: {}. Store the index settings again to correct it.", fieldConfig.getStoredFieldName(),
					e.getMessage());
			return null;
		}
	}

	/**
	 * True when null or absent list elements are filled too, not only a whole missing value.
	 */
	public boolean fillsEachElement() {
		return DefaultValue.Fill.EACH_ELEMENT.equals(fill);
	}
}
