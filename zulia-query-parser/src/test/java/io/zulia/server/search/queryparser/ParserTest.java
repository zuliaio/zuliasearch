package io.zulia.server.search.queryparser;

import io.zulia.message.ZuliaIndex;
import io.zulia.message.ZuliaIndex.FieldConfig.FieldType;
import io.zulia.message.ZuliaQuery.Query.Operator;
import io.zulia.server.analysis.ZuliaPerFieldAnalyzer;
import io.zulia.server.config.ServerIndexConfig;
import io.zulia.server.search.queryparser.legacy.ZuliaLegacyMultiFieldQueryParser;
import org.apache.lucene.search.Query;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.Collection;
import java.util.List;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ParserTest {

	private static ZuliaLegacyMultiFieldQueryParser oldParser;
	private static ZuliaFlexibleQueryParser newParser;

	@BeforeAll
	public static void initAll() {

		ZuliaIndex.IndexSettings.Builder indexSettingBuilder = ZuliaIndex.IndexSettings.newBuilder();
		indexSettingBuilder.setIndexName("test");
		indexSettingBuilder.addFieldConfig(ZuliaIndex.FieldConfig.newBuilder().setStoredFieldName("title").setFieldType(FieldType.STRING)
				.addIndexAs(ZuliaIndex.IndexAs.newBuilder().setIndexFieldName("title").setAnalyzerName("standard").build()).build());
		indexSettingBuilder.addFieldConfig(ZuliaIndex.FieldConfig.newBuilder().setStoredFieldName("abstract").setFieldType(FieldType.STRING)
				.addIndexAs(ZuliaIndex.IndexAs.newBuilder().setIndexFieldName("abstract").setAnalyzerName("standard").build()).build());

		indexSettingBuilder.addFieldConfig(ZuliaIndex.FieldConfig.newBuilder().setStoredFieldName("pubYear").setFieldType(FieldType.NUMERIC_INT)
				.addIndexAs(ZuliaIndex.IndexAs.newBuilder().setIndexFieldName("pubYear").build()).build());

		ServerIndexConfig serverIndexConfig = new ServerIndexConfig(indexSettingBuilder.build());
		ZuliaPerFieldAnalyzer zuliaPerFieldAnalyzer = new ZuliaPerFieldAnalyzer(serverIndexConfig);

		newParser = new ZuliaFlexibleQueryParser(zuliaPerFieldAnalyzer, serverIndexConfig);
		oldParser = new ZuliaLegacyMultiFieldQueryParser(zuliaPerFieldAnalyzer, serverIndexConfig);
	}

	public static Query parse(String query, Collection<String> defaultFields, Operator defaultOperator, int minMatch, ZuliaParser parser1) throws Exception {
		parser1.setDefaultFields(defaultFields);
		parser1.setDefaultOperator(defaultOperator);
		parser1.setMinimumNumberShouldMatch(minMatch);
		Query parse = parser1.parse(query);
		System.out.println(parse);
		return parse;
	}

	@Test
	@Order(2)
	public void basicTest() throws Exception {

		Query q = parse("title:\"lung cancer\"", List.of("field1", "field2"), Operator.OR, 0, oldParser);
		Query q2 = parse("title:\"lung cancer\"", List.of("field1", "field2"), Operator.OR, 0, newParser);
		Assertions.assertEquals(q, q2);

		q = parse("title:\"Lung Cancer\"", List.of("field1", "field2"), Operator.OR, 0, oldParser);
		q2 = parse("title:\"Lung Cancer\"", List.of("field1", "field2"), Operator.OR, 0, newParser);
		Assertions.assertEquals(q, q2);

		q = parse("Lung* Cancer*", List.of("title", "abstract"), Operator.OR, 0, oldParser);
		q2 = parse("Lung* Cancer*", List.of("title", "abstract"), Operator.OR, 0, newParser);
		Assertions.assertEquals(q, q2);

		oldParser.setSplitOnWhitespace(true);
		q = parse("Lung Cancer", List.of("field1", "field2"), Operator.OR, 0, oldParser);
		q2 = parse("Lung Cancer", List.of("field1", "field2"), Operator.OR, 0, newParser);
		oldParser.setSplitOnWhitespace(false);
		Assertions.assertEquals(q, q2);

		oldParser.setSplitOnWhitespace(true);
		q = parse("Cancer Diabetes \"Drug Treatment\"", List.of("field1", "field2"), Operator.OR, 2, oldParser);
		q2 = parse("Cancer Diabetes \"Drug Treatment\"", List.of("field1", "field2"), Operator.OR, 2, newParser);
		oldParser.setSplitOnWhitespace(false);
		Assertions.assertEquals(q, q2);

		oldParser.setSplitOnWhitespace(true);
		q = parse("Cancer Diabetes \"Drug Treatment\"", List.of("title", "abstract"), Operator.OR, 2, oldParser);
		q2 = parse("Cancer Diabetes \"Drug Treatment\"", List.of("title", "abstract"), Operator.OR, 2, newParser);
		oldParser.setSplitOnWhitespace(false);
		Assertions.assertEquals(q, q2);

		oldParser.setSplitOnWhitespace(true);
		q = parse("Cancer Diabetes \"Drug Treatment\"", List.of("title", "abstract"), Operator.OR, 2, oldParser);
		q2 = parse("(Cancer Diabetes \"Drug Treatment\")~2", List.of("title", "abstract"), Operator.OR, 0, newParser);
		oldParser.setSplitOnWhitespace(false);
		Assertions.assertEquals(q, q2);


		oldParser.setSplitOnWhitespace(true);
		q = parse("(Cancer Diabetes \"Drug Treatment\") AND rating:[4.0 TO *]", List.of("title", "abstract"), Operator.OR, 0, oldParser);
		q2 = parse("(Cancer Diabetes \"Drug Treatment\") AND rating:[4.0 TO *]", List.of("title", "abstract"), Operator.OR, 0, newParser);
		oldParser.setSplitOnWhitespace(false);
		Assertions.assertEquals(q, q2);


		q = parse("*:*", List.of("field1", "field2"), Operator.OR, 0, oldParser);
		q2 = parse("*:*", List.of("field1", "field2"), Operator.OR, 0, newParser);
		Assertions.assertEquals(q, q2);

		q = parse("*", List.of("field1", "field2"), Operator.OR, 0, oldParser);
		q2 = parse("*", List.of("field1", "field2"), Operator.OR, 0, newParser);
		Assertions.assertEquals(q, q2);

	}

}
