package io.zulia.fields;

import io.zulia.fields.annotations.AsField;
import io.zulia.fields.annotations.DefaultSearch;
import io.zulia.fields.annotations.DefaultValue;
import io.zulia.fields.annotations.Embedded;
import io.zulia.fields.annotations.Faceted;
import io.zulia.fields.annotations.FacetedFields;
import io.zulia.fields.annotations.Indexed;
import io.zulia.fields.annotations.IndexedFields;
import io.zulia.fields.annotations.OnMalformed;
import io.zulia.fields.annotations.Sorted;
import io.zulia.fields.annotations.SortedFields;
import io.zulia.fields.annotations.UniqueId;
import io.zulia.message.ZuliaIndex;
import io.zulia.message.ZuliaIndex.FacetAs;
import io.zulia.message.ZuliaIndex.FieldConfig;
import io.zulia.message.ZuliaIndex.FieldConfig.FieldType;
import io.zulia.message.ZuliaIndex.IndexAs;
import io.zulia.message.ZuliaIndex.SortAs;
import io.zulia.util.AnnotationUtil;
import io.zulia.util.BooleanUtil;
import io.zulia.util.DefaultValueUtil;
import io.zulia.util.ZuliaDateUtil;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.function.Function;

public class FieldConfigMapper<T> {

	private final String prefix;
	private final Class<T> clazz;
	private final HashMap<String, FieldConfig> fieldConfigMap;
	private final List<FieldConfigMapper<?>> embeddedFieldConfigMappers;

	public FieldConfigMapper(Class<T> clazz, String prefix) {
		this.clazz = clazz;
		this.prefix = prefix;
		this.fieldConfigMap = new HashMap<>();
		this.embeddedFieldConfigMappers = new ArrayList<>();
	}

	public void setupField(Field f) {
		String fieldName = f.getName();

		if (f.isAnnotationPresent(AsField.class)) {
			AsField as = f.getAnnotation(AsField.class);
			fieldName = as.value();
		}

		if (!prefix.isEmpty()) {
			fieldName = prefix + "." + fieldName;
		}

		Class<?> fieldType = f.getType();

		if (List.class.isAssignableFrom(fieldType)) {
			Type genericType = f.getGenericType();
			if (genericType instanceof ParameterizedType pType) {
				fieldType = (Class<?>) pType.getActualTypeArguments()[0];
			}
		}

		if (f.isAnnotationPresent(Embedded.class)) {
			if (f.isAnnotationPresent(IndexedFields.class) || f.isAnnotationPresent(Indexed.class) || f.isAnnotationPresent(Faceted.class)
					|| f.isAnnotationPresent(UniqueId.class) || f.isAnnotationPresent(DefaultSearch.class) || f.isAnnotationPresent(DefaultValue.class)
					|| f.isAnnotationPresent(OnMalformed.class)) {
				throw new RuntimeException(
						"Cannot use Indexed, Faceted, UniqueId, DefaultSearch, DefaultValue, OnMalformed on embedded field <" + f.getName() + "> for class <"
								+ clazz.getSimpleName() + ">");
			}

			FieldConfigMapper<?> fieldConfigMapper = new FieldConfigMapper<>(fieldType, fieldName);

			List<Field> allFields = AnnotationUtil.getNonStaticFields(fieldType, true);

			for (Field ef : allFields) {
				ef.setAccessible(true);
				fieldConfigMapper.setupField(ef);
			}
			embeddedFieldConfigMappers.add(fieldConfigMapper);
		}
		else {
			FieldConfig.Builder fieldConfigBuilder = FieldConfig.newBuilder();
			fieldConfigBuilder.setStoredFieldName(fieldName);

			if (fieldType.equals(String.class)) {
				fieldConfigBuilder.setFieldType(FieldType.STRING);
			}
			else if (fieldType.equals(int.class) || fieldType.equals(Integer.class)) {
				fieldConfigBuilder.setFieldType(FieldType.NUMERIC_INT);
			}
			else if (fieldType.equals(long.class) || fieldType.equals(Long.class)) {
				fieldConfigBuilder.setFieldType(FieldType.NUMERIC_LONG);
			}
			else if (fieldType.equals(float.class) || fieldType.equals(Float.class)) {
				fieldConfigBuilder.setFieldType(FieldType.NUMERIC_FLOAT);
			}
			else if (fieldType.equals(double.class) || fieldType.equals(Double.class)) {
				fieldConfigBuilder.setFieldType(FieldType.NUMERIC_DOUBLE);
			}
			else if (fieldType.equals(boolean.class) || fieldType.equals(Boolean.class)) {
				fieldConfigBuilder.setFieldType(FieldType.BOOL);
			}
			else if (fieldType.equals(Date.class)) {
				fieldConfigBuilder.setFieldType(FieldType.DATE);
			}

			if (f.isAnnotationPresent(DefaultValue.class)) {
				DefaultValue defaultValue = f.getAnnotation(DefaultValue.class);
				fieldConfigBuilder.setDefaultValue(parseDefaultValue(fieldName, fieldType, defaultValue));
			}
			if (f.isAnnotationPresent(OnMalformed.class)) {
				fieldConfigBuilder.setMalformedValueHandling(f.getAnnotation(OnMalformed.class).value());
				// the rule the builder applies at build time, so an annotated class fails here rather than at create index
				DefaultValueUtil.validateMalformedValueHandling(fieldName, fieldConfigBuilder.getFieldType(), fieldConfigBuilder.getMalformedValueHandling(),
						fieldConfigBuilder.hasDefaultValue());
			}

			if (f.isAnnotationPresent(IndexedFields.class)) {
				IndexedFields in = f.getAnnotation(IndexedFields.class);
				for (Indexed indexed : in.value()) {
					addIndexedField(indexed, fieldName, fieldConfigBuilder);
				}
			}
			else if (f.isAnnotationPresent(Indexed.class)) {
				Indexed in = f.getAnnotation(Indexed.class);
				addIndexedField(in, fieldName, fieldConfigBuilder);

			}

			if (f.isAnnotationPresent(FacetedFields.class)) {
				FacetedFields ff = f.getAnnotation(FacetedFields.class);
				for (Faceted faceted : ff.value()) {
					addFacetedField(fieldName, fieldConfigBuilder, faceted);
				}
			}
			if (f.isAnnotationPresent(Faceted.class)) {
				Faceted faceted = f.getAnnotation(Faceted.class);
				addFacetedField(fieldName, fieldConfigBuilder, faceted);
			}

			if (f.isAnnotationPresent(SortedFields.class)) {
				SortedFields sf = f.getAnnotation(SortedFields.class);
				for (Sorted sorted : sf.value()) {
					addSortedField(fieldName, fieldConfigBuilder, sorted);
				}
			}
			else if (f.isAnnotationPresent(Sorted.class)) {
				Sorted sorted = f.getAnnotation(Sorted.class);
				addSortedField(fieldName, fieldConfigBuilder, sorted);
			}

			fieldConfigMap.put(fieldName, fieldConfigBuilder.build());
		}

	}

	/**
	 * The default is parsed against the declared Java field type, which picks the proto case the builder would send.
	 */
	private ZuliaIndex.DefaultValue parseDefaultValue(String fieldName, Class<?> fieldType, DefaultValue defaultValue) {
		String text = defaultValue.value();
		ZuliaIndex.DefaultValue.Builder builder = ZuliaIndex.DefaultValue.newBuilder().setFill(defaultValue.fill());
		if (fieldType.equals(String.class)) {
			builder.setStringValue(text);
		}
		else if (fieldType.equals(int.class) || fieldType.equals(Integer.class)) {
			builder.setIntValue(parseNumber(fieldName, fieldType, text, Integer::parseInt));
		}
		else if (fieldType.equals(long.class) || fieldType.equals(Long.class)) {
			builder.setLongValue(parseNumber(fieldName, fieldType, text, Long::parseLong));
		}
		else if (fieldType.equals(float.class) || fieldType.equals(Float.class)) {
			builder.setFloatValue(parseNumber(fieldName, fieldType, text, Float::parseFloat));
		}
		else if (fieldType.equals(double.class) || fieldType.equals(Double.class)) {
			builder.setDoubleValue(parseNumber(fieldName, fieldType, text, Double::parseDouble));
		}
		else if (fieldType.equals(boolean.class) || fieldType.equals(Boolean.class)) {
			Boolean parsed = BooleanUtil.parseBoolean(text);
			if (parsed == null) {
				throw unparseableDefault(fieldName, fieldType, text, "expected true or false");
			}
			builder.setBoolValue(parsed);
		}
		else if (fieldType.equals(Date.class)) {
			Long epochMilli = ZuliaDateUtil.parseToEpochMilli(text);
			if (epochMilli == null) {
				throw unparseableDefault(fieldName, fieldType, text, "supported formats: " + ZuliaDateUtil.SUPPORTED_DATE_STRING_FORMATS);
			}
			builder.setDateValue(epochMilli);
		}
		else {
			throw new IllegalArgumentException("Field <" + fieldName + "> of class <" + clazz.getSimpleName() + "> has Java type <" + fieldType.getSimpleName()
					+ "> which does not support @DefaultValue");
		}
		return builder.build();
	}

	private <N extends Number> N parseNumber(String fieldName, Class<?> fieldType, String text, Function<String, N> parser) {
		try {
			return parser.apply(text);
		}
		catch (NumberFormatException e) {
			throw unparseableDefault(fieldName, fieldType, text, e.getMessage());
		}
	}

	private IllegalArgumentException unparseableDefault(String fieldName, Class<?> fieldType, String text, String reason) {
		return new IllegalArgumentException(
				"Field <" + fieldName + "> of class <" + clazz.getSimpleName() + "> is <" + fieldType.getSimpleName() + "> but @DefaultValue <" + text
						+ "> cannot be parsed as that type (" + reason + ")");
	}

	private void addIndexedField(Indexed in, String fieldName, FieldConfig.Builder fieldConfigBuilder) {
		String analyzerName = in.analyzerName();

		String indexedFieldName = fieldName;
		if (!in.fieldName().isEmpty()) {
			indexedFieldName = in.fieldName();
		}

		IndexAs.Builder builder = IndexAs.newBuilder().setIndexFieldName(indexedFieldName);
		if (!analyzerName.isEmpty()) {
			builder.setAnalyzerName(analyzerName);
		}
		fieldConfigBuilder.addIndexAs(builder);
	}

	private void addFacetedField(String fieldName, FieldConfig.Builder fieldConfigBuilder, Faceted faceted) {
		String facetName = fieldName;
		if (!faceted.name().isEmpty()) {
			facetName = faceted.name();
		}

		FacetAs.DateHandling dateHandling = faceted.dateHandling();

		FacetAs.Builder builder = FacetAs.newBuilder().setFacetName(facetName);
		builder.setDateHandling(dateHandling);
		fieldConfigBuilder.addFacetAs(builder);
	}

	private void addSortedField(String fieldName, FieldConfig.Builder fieldConfigBuilder, Sorted sorted) {
		String sortFieldName = fieldName;
		if (!sorted.fieldName().isEmpty()) {
			sortFieldName = sorted.fieldName();
		}
		SortAs.Builder builder = SortAs.newBuilder().setSortFieldName(sortFieldName);
		builder.setStringHandling(sorted.stringHandling());
		fieldConfigBuilder.addSortAs(builder);
	}

	public List<FieldConfig> getFieldConfigs() {
		List<FieldConfig> configs = new ArrayList<>();
		for (String fieldName : fieldConfigMap.keySet()) {
			FieldConfig fieldConfig = fieldConfigMap.get(fieldName);
			configs.add(fieldConfig);
		}
		for (FieldConfigMapper<?> fcm : embeddedFieldConfigMappers) {
			configs.addAll(fcm.getFieldConfigs());
		}
		return configs;
	}

}
