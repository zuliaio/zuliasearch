package io.zulia.server.search.queryparser;

import io.zulia.message.ZuliaIndex;
import io.zulia.message.ZuliaIndex.FieldConfig;
import io.zulia.message.ZuliaIndex.FieldConfig.FieldType;
import io.zulia.message.ZuliaIndex.IndexAs;
import io.zulia.message.ZuliaQuery.Query.Operator;
import io.zulia.server.analysis.ZuliaPerFieldAnalyzer;
import io.zulia.server.config.ServerIndexConfig;
import io.zulia.server.search.queryparser.legacy.ZuliaLegacyMultiFieldQueryParser;
import org.apache.lucene.search.Query;

import java.util.Collection;
import java.util.List;

public class NewQueryParserTest {

	public static void main(String[] args) throws Exception {

		ZuliaIndex.IndexSettings.Builder indexSettingBuilder = ZuliaIndex.IndexSettings.newBuilder();
		indexSettingBuilder.setIndexName("test");
		indexSettingBuilder.addFieldConfig(FieldConfig.newBuilder().setStoredFieldName("title").setFieldType(FieldType.STRING)
				.addIndexAs(IndexAs.newBuilder().setIndexFieldName("title").setAnalyzerName("standard").build()).build());
		indexSettingBuilder.addFieldConfig(FieldConfig.newBuilder().setStoredFieldName("pubYear").setFieldType(FieldType.DATE)
				.addIndexAs(IndexAs.newBuilder().setIndexFieldName("pubYear").build()).build());

		ServerIndexConfig serverIndexConfig = new ServerIndexConfig(indexSettingBuilder.build());
		ZuliaPerFieldAnalyzer zuliaPerFieldAnalyzer = new ZuliaPerFieldAnalyzer(serverIndexConfig);

		ZuliaParser newParser = new ZuliaFlexibleQueryParser(zuliaPerFieldAnalyzer, serverIndexConfig);
		ZuliaParser oldParser = new ZuliaLegacyMultiFieldQueryParser(zuliaPerFieldAnalyzer, serverIndexConfig);

		parseWithBoth("\"lung cancer\" \"breast cancer\"", List.of("title", "abstract"), Operator.AND, 0, oldParser, newParser);

		parseWithBoth("\"lung cancer\" \"breast cancer\"", List.of("title", "abstract"), Operator.AND, 0, oldParser, newParser);

		newParser.setDefaultFields(List.of("default1", "pubYear2"));
		newParser.setDefaultOperator(Operator.AND);

		Query query;
		query = newParser.parse("title,authors.firstName:(\"lung cancer\" OR diabetes OR stuff)~2 AND b AND pubYear<=2015-01-01");
		System.out.println(query);

		newParser.setMinimumNumberShouldMatch(2);
		query = newParser.parse("\"ice cream\" pubYear:2000 soda cola other AND |author|:[0 TO 5]");
		System.out.println(query);

	}

	public static void parseWithBoth(String query, Collection<String> defaultFields, Operator defaultOperator, int minMatch, ZuliaParser parser1,
			ZuliaParser parser2) throws Exception {
		Query q = parse(query, defaultFields, defaultOperator, minMatch, parser1);
		System.out.println(q);

		Query q2 = parse(query, defaultFields, defaultOperator, minMatch, parser2);
		System.out.println(q2);

	}

	public static Query parse(String query, Collection<String> defaultFields, Operator defaultOperator, int minMatch, ZuliaParser parser1) throws Exception {
		parser1.setDefaultFields(defaultFields);
		parser1.setDefaultOperator(defaultOperator);
		parser1.setMinimumNumberShouldMatch(minMatch);
		Query q = parser1.parse(query);
		return q;
	}
}
