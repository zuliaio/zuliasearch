package io.zulia.client;

import io.zulia.fields.FieldConfigBuilder;
import io.zulia.fields.FieldConfigMapper;
import io.zulia.fields.annotations.DefaultValue;
import io.zulia.fields.annotations.Embedded;
import io.zulia.fields.annotations.Faceted;
import io.zulia.fields.annotations.Indexed;
import io.zulia.fields.annotations.OnMalformed;
import io.zulia.message.ZuliaIndex;
import io.zulia.message.ZuliaIndex.FieldConfig;
import io.zulia.message.ZuliaIndex.FieldConfig.MalformedValueHandling;
import io.zulia.util.AnnotationUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.lang.reflect.Field;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A default whose Java type is not the field's one type fails at build time. The mapper parses annotation text by
 * the declared Java type.
 */
public class FieldDefaultValueBuilderTest {

	@Test
	public void eachTypeTakesExactlyItsOwnCase() {
		Date epoch = new Date(0);
		Assertions.assertEquals("(none)", FieldConfigBuilder.createString("s").defaultValue("(none)").build().getDefaultValue().getStringValue());
		Assertions.assertEquals(0, FieldConfigBuilder.createInt("i").defaultValue(0).build().getDefaultValue().getIntValue());
		Assertions.assertEquals(0L, FieldConfigBuilder.createLong("l").defaultValue(0L).build().getDefaultValue().getLongValue());
		Assertions.assertEquals(0.0f, FieldConfigBuilder.createFloat("f").defaultValue(0.0f).build().getDefaultValue().getFloatValue());
		Assertions.assertEquals(1.0d, FieldConfigBuilder.createDouble("d").defaultValue(1.0d).build().getDefaultValue().getDoubleValue());
		Assertions.assertFalse(FieldConfigBuilder.createBool("b").defaultValue(false).build().getDefaultValue().getBoolValue());
		Assertions.assertEquals(0L, FieldConfigBuilder.createDate("t").defaultValue(epoch).build().getDefaultValue().getDateValue());
		ZuliaIndex.GeoPointValue point = FieldConfigBuilder.createGeoPoint("g").defaultValue(40.7128, -74.006).build().getDefaultValue().getGeoPointValue();
		Assertions.assertEquals(40.7128, point.getLatitude());
		Assertions.assertEquals(-74.006, point.getLongitude());

		FieldConfig eachElement = FieldConfigBuilder.createString("authors.affiliation").defaultValue("(none)", ZuliaIndex.DefaultValue.Fill.EACH_ELEMENT)
				.build();
		Assertions.assertEquals(ZuliaIndex.DefaultValue.Fill.EACH_ELEMENT, eachElement.getDefaultValue().getFill());
		FieldConfig wholeValue = FieldConfigBuilder.createInt("i").defaultValue(0).build();
		Assertions.assertEquals(ZuliaIndex.DefaultValue.Fill.WHOLE_VALUE, wholeValue.getDefaultValue().getFill());
		Assertions.assertFalse(FieldConfigBuilder.createInt("i").build().hasDefaultValue());
	}

	@Test
	public void mismatchedTypesFailAtBuildTime() {
		assertRejected(() -> FieldConfigBuilder.createLong("bytes").defaultValue(0),
				"Field <bytes> is NUMERIC_LONG but defaultValue is intValue <0>, expected longValue");
		assertRejected(() -> FieldConfigBuilder.createInt("count").defaultValue("zero"),
				"Field <count> is NUMERIC_INT but defaultValue is stringValue <zero>, expected intValue");
		assertRejected(() -> FieldConfigBuilder.createFloat("score").defaultValue(0.0),
				"Field <score> is NUMERIC_FLOAT but defaultValue is doubleValue <0.0>, expected floatValue");
		assertRejected(() -> FieldConfigBuilder.createDouble("weight").defaultValue(1.0f),
				"Field <weight> is NUMERIC_DOUBLE but defaultValue is floatValue <1.0>, expected doubleValue");
		assertRejected(() -> FieldConfigBuilder.createBool("active").defaultValue(0),
				"Field <active> is BOOL but defaultValue is intValue <0>, expected boolValue");
		assertRejected(() -> FieldConfigBuilder.createDate("when").defaultValue(0L),
				"Field <when> is DATE but defaultValue is longValue <0>, expected dateValue");
		assertRejected(() -> FieldConfigBuilder.createString("title").defaultValue(true),
				"Field <title> is STRING but defaultValue is boolValue <true>, expected stringValue");
		assertRejected(() -> FieldConfigBuilder.createVector("v").defaultValue(0.0f),
				"Field <v> is VECTOR which does not support a defaultValue, found floatValue <0.0>");
		assertRejected(() -> FieldConfigBuilder.createUnitVector("u").defaultValue("x"), "Field <u> is UNIT_VECTOR which does not support a defaultValue");
		assertRejected(() -> FieldConfigBuilder.createGeoPoint("g").defaultValue("x"),
				"Field <g> is GEO_POINT but defaultValue is stringValue <x>, expected geoPointValue");
		assertRejected(() -> FieldConfigBuilder.createGeoPoint("g").defaultValue(91.0, 0.0),
				"Field <g> defaultValue latitude <91.0> must be between -90 and 90");
		assertRejected(() -> FieldConfigBuilder.createGeoPoint("g").defaultValue(0.0, -181.0),
				"Field <g> defaultValue longitude <-181.0> must be between -180 and 180");
	}

	@Test
	public void nullArgumentsNameTheField() {
		assertRejected(() -> FieldConfigBuilder.createString("journal").defaultValue((String) null), "Field <journal> defaultValue value cannot be null");
		assertRejected(() -> FieldConfigBuilder.createDate("pubDate").defaultValue((Date) null), "Field <pubDate> defaultValue value cannot be null");
		assertRejected(() -> FieldConfigBuilder.createInt("citationCount").defaultValue(0, null), "Field <citationCount> defaultValue fill cannot be null");
		assertRejected(() -> FieldConfigBuilder.createGeoPoint("location").defaultValue(1.0, 2.0, null), "Field <location> defaultValue fill cannot be null");
	}

	@Test
	public void malformedHandling() {
		Assertions.assertEquals(MalformedValueHandling.SKIP,
				FieldConfigBuilder.createInt("i").onMalformed(MalformedValueHandling.SKIP).build().getMalformedValueHandling());
		Assertions.assertEquals(MalformedValueHandling.FAIL, FieldConfigBuilder.createInt("i").build().getMalformedValueHandling());
		Assertions.assertEquals(MalformedValueHandling.USE_DEFAULT,
				FieldConfigBuilder.createInt("i").onMalformed(MalformedValueHandling.USE_DEFAULT).defaultValue(0).build().getMalformedValueHandling());

		assertRejected(() -> FieldConfigBuilder.createInt("i").onMalformed(MalformedValueHandling.USE_DEFAULT).build(),
				"Field <i> has malformedValueHandling USE_DEFAULT but no defaultValue is set");
		// SKIP needs no default, so every type takes it. USE_DEFAULT needs a default and vectors have none
		Assertions.assertEquals(MalformedValueHandling.SKIP,
				FieldConfigBuilder.createVector("v").onMalformed(MalformedValueHandling.SKIP).build().getMalformedValueHandling());
		assertRejected(() -> FieldConfigBuilder.createVector("v").onMalformed(MalformedValueHandling.USE_DEFAULT).build(),
				"Field <v> is VECTOR which takes no defaultValue, so malformedValueHandling USE_DEFAULT is not available");
		assertRejected(() -> FieldConfigBuilder.createGeoPoint("g").onMalformed(MalformedValueHandling.USE_DEFAULT).build(),
				"Field <g> has malformedValueHandling USE_DEFAULT but no defaultValue is set");
		Assertions.assertEquals(MalformedValueHandling.USE_DEFAULT,
				FieldConfigBuilder.createGeoPoint("g").defaultValue(1.0, 2.0).onMalformed(MalformedValueHandling.USE_DEFAULT).build()
						.getMalformedValueHandling());
	}

	public static class Annotated {

		@Indexed
		@DefaultValue("(none)")
		private String journal;

		@Indexed
		@Faceted
		@DefaultValue("0")
		@OnMalformed(MalformedValueHandling.USE_DEFAULT)
		private int citationCount;

		@Indexed
		@DefaultValue("0")
		private Long bytes;

		@Indexed
		@DefaultValue("0.5")
		private float score;

		@Indexed
		@DefaultValue("1.5")
		private Double weight;

		@Indexed
		@DefaultValue("false")
		private boolean active;

		@Indexed
		@DefaultValue("1970-01-01")
		private Date pubDate;

		@Indexed
		@DefaultValue(value = "untagged", fill = ZuliaIndex.DefaultValue.Fill.EACH_ELEMENT)
		private List<String> tags;

		@Indexed
		@OnMalformed(MalformedValueHandling.SKIP)
		private int lenient;
	}

	@Test
	public void annotationsParseAgainstTheJavaType() {
		Map<String, FieldConfig> configs = mapped(Annotated.class);

		Assertions.assertEquals("(none)", configs.get("journal").getDefaultValue().getStringValue());
		Assertions.assertEquals(0, configs.get("citationCount").getDefaultValue().getIntValue());
		Assertions.assertEquals(MalformedValueHandling.USE_DEFAULT, configs.get("citationCount").getMalformedValueHandling());
		Assertions.assertEquals(0L, configs.get("bytes").getDefaultValue().getLongValue());
		Assertions.assertEquals(ZuliaIndex.DefaultValue.ValueCase.LONGVALUE, configs.get("bytes").getDefaultValue().getValueCase());
		Assertions.assertEquals(0.5f, configs.get("score").getDefaultValue().getFloatValue());
		Assertions.assertEquals(1.5d, configs.get("weight").getDefaultValue().getDoubleValue());
		Assertions.assertFalse(configs.get("active").getDefaultValue().getBoolValue());
		Assertions.assertEquals(0L, configs.get("pubDate").getDefaultValue().getDateValue());
		Assertions.assertEquals("untagged", configs.get("tags").getDefaultValue().getStringValue());
		Assertions.assertEquals(ZuliaIndex.DefaultValue.Fill.EACH_ELEMENT, configs.get("tags").getDefaultValue().getFill());
		Assertions.assertEquals(MalformedValueHandling.SKIP, configs.get("lenient").getMalformedValueHandling());
		Assertions.assertFalse(configs.get("lenient").hasDefaultValue());
	}

	public static class BadInt {

		@Indexed
		@DefaultValue("zero")
		private int count;
	}

	public static class BadBool {

		@Indexed
		@DefaultValue("maybe")
		private boolean active;
	}

	public static class BadDate {

		@Indexed
		@DefaultValue("yesterday")
		private Date when;
	}

	public static class Unsupported {

		@Indexed
		@DefaultValue("x")
		private StringBuilder odd;
	}

	@Test
	public void annotationsThatDoNotParseAreRejected() {
		assertRejected(() -> mapped(BadInt.class), "Field <count> of class <BadInt> is <int> but @DefaultValue <zero> cannot be parsed as that type");
		assertRejected(() -> mapped(BadBool.class),
				"Field <active> of class <BadBool> is <boolean> but @DefaultValue <maybe> cannot be parsed as that type (expected true or false)");
		assertRejected(() -> mapped(BadDate.class),
				"Field <when> of class <BadDate> is <Date> but @DefaultValue <yesterday> cannot be parsed as that type (supported formats: ");
		assertRejected(() -> mapped(Unsupported.class),
				"Field <odd> of class <Unsupported> has Java type <StringBuilder> which does not support @DefaultValue");
	}

	public static class Sub {

		@Indexed
		private String name;
	}

	public static class DefaultOnEmbedded {

		@Embedded
		@DefaultValue("(none)")
		private Sub sub;
	}

	public static class MalformedOnEmbedded {

		@Embedded
		@OnMalformed(MalformedValueHandling.SKIP)
		private Sub sub;
	}

	public static class UseDefaultWithoutDefault {

		@Indexed
		@OnMalformed(MalformedValueHandling.USE_DEFAULT)
		private int count;
	}

	@Test
	public void theTwoAnnotationsAreRejectedOnAnEmbeddedFieldRatherThanIgnored() {
		RuntimeException onDefault = Assertions.assertThrows(RuntimeException.class, () -> mapped(DefaultOnEmbedded.class));
		Assertions.assertTrue(onDefault.getMessage().contains("DefaultValue, OnMalformed on embedded field <sub> for class <DefaultOnEmbedded>"),
				onDefault.getMessage());
		RuntimeException onMalformed = Assertions.assertThrows(RuntimeException.class, () -> mapped(MalformedOnEmbedded.class));
		Assertions.assertTrue(onMalformed.getMessage().contains("on embedded field <sub> for class <MalformedOnEmbedded>"), onMalformed.getMessage());
	}

	@Test
	public void theMapperAppliesTheSameUseDefaultRuleAsTheBuilder() {
		assertRejected(() -> mapped(UseDefaultWithoutDefault.class), "Field <count> has malformedValueHandling USE_DEFAULT but no defaultValue is set");
	}

	private static <T> Map<String, FieldConfig> mapped(Class<T> clazz) {
		FieldConfigMapper<T> mapper = new FieldConfigMapper<>(clazz, "");
		for (Field field : AnnotationUtil.getNonStaticFields(clazz, true)) {
			field.setAccessible(true);
			mapper.setupField(field);
		}
		return mapper.getFieldConfigs().stream().collect(Collectors.toMap(FieldConfig::getStoredFieldName, Function.identity()));
	}

	private static void assertRejected(Executable executable, String expectedMessage) {
		IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class, executable);
		Assertions.assertTrue(exception.getMessage().contains(expectedMessage), exception.getMessage());
	}
}
