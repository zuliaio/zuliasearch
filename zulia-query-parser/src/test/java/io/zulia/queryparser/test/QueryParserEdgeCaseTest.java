package io.zulia.queryparser.test;

import io.zulia.DefaultAnalyzers;
import io.zulia.message.ZuliaIndex;
import io.zulia.server.analysis.ZuliaPerFieldAnalyzer;
import io.zulia.server.config.ServerIndexConfig;
import io.zulia.server.search.queryparser.ZuliaFlexibleQueryParser;
import io.zulia.server.search.queryparser.parser.EscapeQuerySyntaxImpl;
import io.zulia.server.search.queryparser.parser.ParseException;
import org.apache.lucene.queryparser.flexible.core.parser.EscapeQuerySyntax;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Locale;

public class QueryParserEdgeCaseTest {

	@Test
	public void invalidFieldBoostTest() {
		ZuliaFlexibleQueryParser parser = parser();

		IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class, () -> parser.setDefaultFields(List.of("title^abc")));
		Assertions.assertEquals("Invalid boost <abc> for query field <title^abc>. Boost must be a number, for example title^2", e.getMessage());

		e = Assertions.assertThrows(IllegalArgumentException.class, () -> parser.setDefaultFields(List.of("title^")));
		Assertions.assertEquals("Invalid boost <> for query field <title^>. Boost must be a number, for example title^2", e.getMessage());
	}

	@Test
	public void validFieldBoostTest() throws Exception {
		ZuliaFlexibleQueryParser parser = parser();
		parser.setDefaultFields(List.of("abstract", "title^2"));

		Query query = parser.parse("cancer");
		Assertions.assertEquals("abstract:cancer (title:cancer)^2.0", query.toString());
	}

	@Test
	public void fractionalWholeNumberTokenTest() {
		ZuliaFlexibleQueryParser parser = parser();
		parser.setDefaultFields(List.of("title"));

		// a fractional minimum should match is a typed ParseException instead of a raw NumberFormatException escaping parse
		// the whole number detail from parseInt is replaced by the generic syntax error because ZuliaSyntaxParser.parse calls setQuery on the exception
		ParseException minMatch = Assertions.assertThrows(ParseException.class, () -> parser.parse("title:(cancer lung)@2.5"));
		Assertions.assertTrue(minMatch.getMessage().contains("title:(cancer lung)@2.5"), minMatch.getMessage());

		// phrase slop takes a whole number as well
		ParseException slop = Assertions.assertThrows(ParseException.class, () -> parser.parse("title:\"cancer lung\"~1.5"));
		Assertions.assertTrue(slop.getMessage().contains("title:\"cancer lung\"~1.5"), slop.getMessage());
	}

	@Test
	public void numericSetInvalidValueTest() {
		ZuliaFlexibleQueryParser parser = parser();

		IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class, () -> parser.parse("count:zl:ns(1 abc 3)"));
		Assertions.assertEquals("Invalid value <abc> for numeric set query on int field <count>. Every value must be a valid int", e.getMessage());

		// a valid number that is not a whole number still fails for an int field
		e = Assertions.assertThrows(IllegalArgumentException.class, () -> parser.parse("count:zl:ns(1 2.5 3)"));
		Assertions.assertEquals("Invalid value <2.5> for numeric set query on int field <count>. Every value must be a valid int", e.getMessage());

		// one past Long.MAX_VALUE overflows the long parse
		e = Assertions.assertThrows(IllegalArgumentException.class, () -> parser.parse("longField:zl:ns(9223372036854775808)"));
		Assertions.assertEquals("Invalid value <9223372036854775808> for numeric set query on long field <longField>. Every value must be a valid long",
				e.getMessage());

		e = Assertions.assertThrows(IllegalArgumentException.class, () -> parser.parse("floatField:zl:ns(notafloat)"));
		Assertions.assertEquals("Invalid value <notafloat> for numeric set query on float field <floatField>. Every value must be a valid float",
				e.getMessage());
	}

	@Test
	public void numericSetValidTest() throws Exception {
		ZuliaFlexibleQueryParser parser = parser();

		Query query = parser.parse("count:zl:ns(1 2 3)");
		Assertions.assertEquals("count_NUMERIC_INT:{1 2 3}", query.toString());
	}

	@Test
	public void exclusiveRangeAtIntExtremesTest() throws Exception {
		ZuliaFlexibleQueryParser parser = parser();

		// an exclusive lower bound of Integer.MAX_VALUE used to overflow to Integer.MIN_VALUE and match everything
		Query lowerAtMax = parser.parse("count:{2147483647 TO 2147483647]");
		Assertions.assertInstanceOf(MatchNoDocsQuery.class, lowerAtMax);

		Query upperAtMin = parser.parse("count:[-2147483648 TO -2147483648}");
		Assertions.assertInstanceOf(MatchNoDocsQuery.class, upperAtMin);

		// one step inside the extreme still builds a normal range
		Query stillMatchesMax = parser.parse("count:{2147483646 TO 2147483647]");
		Assertions.assertEquals("count_NUMERIC_INT:[2147483647 TO 2147483647]", stillMatchesMax.toString());
	}

	@Test
	public void exclusiveRangeAtLongExtremesTest() throws Exception {
		ZuliaFlexibleQueryParser parser = parser();

		Query lowerAtMax = parser.parse("longField:{9223372036854775807 TO 9223372036854775807]");
		Assertions.assertInstanceOf(MatchNoDocsQuery.class, lowerAtMax);

		Query upperAtMin = parser.parse("longField:[-9223372036854775808 TO -9223372036854775808}");
		Assertions.assertInstanceOf(MatchNoDocsQuery.class, upperAtMin);

		Query stillMatchesMax = parser.parse("longField:{9223372036854775806 TO 9223372036854775807]");
		Assertions.assertEquals("longField_NUMERIC_LONG:[9223372036854775807 TO 9223372036854775807]", stillMatchesMax.toString());
	}

	@Test
	public void escapeQuotedTest() {
		EscapeQuerySyntaxImpl escaper = new EscapeQuerySyntaxImpl();

		// quoted strings only escape quotes
		Assertions.assertEquals("say \\\"hi\\\"", escaper.escape("say \"hi\"", Locale.ROOT, EscapeQuerySyntax.Type.STRING).toString());
		Assertions.assertEquals("12:30", escaper.escape("12:30", Locale.ROOT, EscapeQuerySyntax.Type.STRING).toString());

		// terms escape the full parser syntax set
		Assertions.assertEquals("12\\:30", escaper.escape("12:30", Locale.ROOT, EscapeQuerySyntax.Type.NORMAL).toString());
	}

	@Test
	public void missingDefaultFieldsTest() {
		ZuliaFlexibleQueryParser parser = parser();

		// a fieldless term without default fields reports the missing configuration instead of a NullPointerException
		IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class, () -> parser.parse("cancer"));
		Assertions.assertTrue(e.getMessage().contains("MULTI_FIELDS should be set"), e.getMessage());
	}

	private static ZuliaFlexibleQueryParser parser() {
		ZuliaIndex.IndexSettings.Builder builder = ZuliaIndex.IndexSettings.newBuilder();
		builder.addFieldConfig(stringField("title"));
		builder.addFieldConfig(stringField("abstract"));
		builder.addFieldConfig(numericField("count", ZuliaIndex.FieldConfig.FieldType.NUMERIC_INT));
		builder.addFieldConfig(numericField("longField", ZuliaIndex.FieldConfig.FieldType.NUMERIC_LONG));
		builder.addFieldConfig(numericField("floatField", ZuliaIndex.FieldConfig.FieldType.NUMERIC_FLOAT));

		ServerIndexConfig serverIndexConfig = new ServerIndexConfig(builder.build());
		return new ZuliaFlexibleQueryParser(new ZuliaPerFieldAnalyzer(serverIndexConfig), serverIndexConfig);
	}

	private static ZuliaIndex.FieldConfig stringField(String name) {
		return ZuliaIndex.FieldConfig.newBuilder()
				.addIndexAs(ZuliaIndex.IndexAs.newBuilder().setAnalyzerName(DefaultAnalyzers.STANDARD).setIndexFieldName(name).build())
				.setFieldType(ZuliaIndex.FieldConfig.FieldType.STRING).setStoredFieldName(name).build();
	}

	private static ZuliaIndex.FieldConfig numericField(String name, ZuliaIndex.FieldConfig.FieldType type) {
		return ZuliaIndex.FieldConfig.newBuilder().addIndexAs(ZuliaIndex.IndexAs.newBuilder().setIndexFieldName(name).build()).setFieldType(type)
				.setStoredFieldName(name).build();
	}

}
