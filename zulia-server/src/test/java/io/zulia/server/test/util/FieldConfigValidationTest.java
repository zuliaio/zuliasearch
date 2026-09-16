package io.zulia.server.test.util;

import io.zulia.message.ZuliaIndex.DefaultValue;
import io.zulia.message.ZuliaIndex.FacetAs;
import io.zulia.message.ZuliaIndex.FieldConfig;
import io.zulia.message.ZuliaIndex.FieldConfig.FieldType;
import io.zulia.message.ZuliaIndex.GeoPointValue;
import io.zulia.message.ZuliaIndex.IndexAs;
import io.zulia.message.ZuliaIndex.IndexSettings;
import io.zulia.message.ZuliaIndex.SortAs;
import io.zulia.server.config.ServerIndexConfig;
import io.zulia.server.connection.server.validation.CreateIndexRequestValidator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * A field config that cannot work is rejected when the index is created or updated. A field config that somehow reached
 * storage without being validated must still let the index load, so one bad field never takes an index offline.
 */
public class FieldConfigValidationTest {

	private static IndexSettings.Builder settings(FieldConfig fieldConfig) {
		return IndexSettings.newBuilder().setIndexName("validation").setNumberOfShards(1).addFieldConfig(fieldConfig);
	}

	private static void assertRejected(FieldConfig fieldConfig, String expectedMessage) {
		IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class,
				() -> CreateIndexRequestValidator.validateIndexSettingsAndSetDefaults(settings(fieldConfig)));
		Assertions.assertTrue(exception.getMessage().contains(expectedMessage), exception.getMessage());
	}

	@Test
	public void aVectorFieldTakesNeitherAFacetNorASort() {
		FieldConfig.Builder vector = FieldConfig.newBuilder().setStoredFieldName("embedding").setFieldType(FieldType.VECTOR)
				.addIndexAs(IndexAs.newBuilder().setIndexFieldName("embedding"));

		assertRejected(vector.clone().addFacetAs(FacetAs.newBuilder().setFacetName("embedding")).build(), "VECTOR field <embedding> does not support faceting");
		assertRejected(vector.clone().addSortAs(SortAs.newBuilder().setSortFieldName("embedding")).build(),
				"VECTOR field <embedding> does not support sorting");

		FieldConfig.Builder unitVector = FieldConfig.newBuilder().setStoredFieldName("v").setFieldType(FieldType.UNIT_VECTOR)
				.addIndexAs(IndexAs.newBuilder().setIndexFieldName("v"));
		assertRejected(unitVector.clone().addFacetAs(FacetAs.newBuilder().setFacetName("v")).build(), "UNIT_VECTOR field <v> does not support faceting");
		assertRejected(unitVector.clone().addSortAs(SortAs.newBuilder().setSortFieldName("v")).build(), "UNIT_VECTOR field <v> does not support sorting");

		Assertions.assertDoesNotThrow(() -> CreateIndexRequestValidator.validateIndexSettingsAndSetDefaults(settings(vector.build())));
	}

	@Test
	public void anUnrecognizedFieldTypeIsRejected() {
		// nothing downstream can index it, so the config is refused rather than failing every document
		assertRejected(
				FieldConfig.newBuilder().setStoredFieldName("odd").setFieldTypeValue(77).addIndexAs(IndexAs.newBuilder().setIndexFieldName("odd")).build(),
				"Field <odd> has an unrecognized fieldType <77>");
	}

	@Test
	public void anUnrecognizedFillIsRejected() {
		// a fill value a newer client invented would otherwise behave as WHOLE_VALUE and quietly stop filling list elements
		assertRejected(FieldConfig.newBuilder().setStoredFieldName("count").setFieldType(FieldType.NUMERIC_INT)
						.addIndexAs(IndexAs.newBuilder().setIndexFieldName("count")).setDefaultValue(DefaultValue.newBuilder().setIntValue(0).setFillValue(99)).build(),
				"Field <count> defaultValue has an unrecognized fill <99>");
	}

	@Test
	public void anUnvalidatedDefaultIsDroppedRatherThanStoppingTheIndexFromLoading() {
		// shapes validation rejects, so they can only arrive from settings written without it
		FieldConfig noCaseSet = FieldConfig.newBuilder().setStoredFieldName("count").setFieldType(FieldType.NUMERIC_INT)
				.addIndexAs(IndexAs.newBuilder().setIndexFieldName("count")).setDefaultValue(DefaultValue.getDefaultInstance()).build();
		FieldConfig coordinateOutOfRange = FieldConfig.newBuilder().setStoredFieldName("location").setFieldType(FieldType.GEO_POINT)
				.addIndexAs(IndexAs.newBuilder().setIndexFieldName("location"))
				.setDefaultValue(DefaultValue.newBuilder().setGeoPointValue(GeoPointValue.newBuilder().setLatitude(91).setLongitude(0))).build();
		FieldConfig caseDisagreesWithType = FieldConfig.newBuilder().setStoredFieldName("count2").setFieldType(FieldType.NUMERIC_INT)
				.addIndexAs(IndexAs.newBuilder().setIndexFieldName("count2")).setDefaultValue(DefaultValue.newBuilder().setStringValue("zero")).build();

		for (FieldConfig fieldConfig : java.util.List.of(noCaseSet, coordinateOutOfRange, caseDisagreesWithType)) {
			ServerIndexConfig loaded = Assertions.assertDoesNotThrow(() -> new ServerIndexConfig(settings(fieldConfig).build()),
					"a bad default must not take the index offline");
			Assertions.assertNull(loaded.getFieldDefault(fieldConfig.getStoredFieldName()),
					"the bad default is dropped so the field behaves as if it had none");
		}

		FieldConfig valid = FieldConfig.newBuilder().setStoredFieldName("count3").setFieldType(FieldType.NUMERIC_INT)
				.addIndexAs(IndexAs.newBuilder().setIndexFieldName("count3")).setDefaultValue(DefaultValue.newBuilder().setIntValue(0)).build();
		Assertions.assertEquals(0, new ServerIndexConfig(settings(valid).build()).getFieldDefault("count3").value());
	}
}
