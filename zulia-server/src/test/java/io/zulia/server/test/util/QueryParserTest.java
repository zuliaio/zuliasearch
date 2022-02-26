package io.zulia.server.test.util;

import io.zulia.DefaultAnalyzers;
import io.zulia.message.ZuliaIndex;
import io.zulia.server.analysis.ZuliaPerFieldAnalyzer;
import io.zulia.server.config.ServerIndexConfig;
import io.zulia.server.search.queryparser.legacy.ZuliaLegacyMultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;

import java.util.Arrays;

import static io.zulia.message.ZuliaIndex.AnalyzerSettings.Filter.ASCII_FOLDING;
import static io.zulia.message.ZuliaIndex.AnalyzerSettings.Filter.BRITISH_US;
import static io.zulia.message.ZuliaIndex.AnalyzerSettings.Filter.CASE_PROTECTED_WORDS;
import static io.zulia.message.ZuliaIndex.AnalyzerSettings.Filter.ENGLISH_MIN_STEM;
import static io.zulia.message.ZuliaIndex.AnalyzerSettings.Filter.ENGLISH_POSSESSIVE;
import static io.zulia.message.ZuliaIndex.AnalyzerSettings.Filter.LOWERCASE;

public class QueryParserTest {

	public static void main(String[] args) throws ParseException {

		ZuliaIndex.AnalyzerSettings entitySettings = ZuliaIndex.AnalyzerSettings.newBuilder().setName("entity").addFilter(LOWERCASE).addFilter(ASCII_FOLDING)
				.build();

		ZuliaIndex.AnalyzerSettings textAnalyzer = ZuliaIndex.AnalyzerSettings.newBuilder().setName("text").addFilter(CASE_PROTECTED_WORDS).addFilter(LOWERCASE)
				.addFilter(ASCII_FOLDING).addFilter(ENGLISH_POSSESSIVE).addFilter(ENGLISH_MIN_STEM).addFilter(BRITISH_US).build();

		ZuliaIndex.FieldConfig titleConfig = ZuliaIndex.FieldConfig.newBuilder().setStoredFieldName("title")
				.addIndexAs(ZuliaIndex.IndexAs.newBuilder().setIndexFieldName("title").setAnalyzerName("text").build()).build();

		ZuliaIndex.FieldConfig issn = ZuliaIndex.FieldConfig.newBuilder().setStoredFieldName("issn")
				.addIndexAs(ZuliaIndex.IndexAs.newBuilder().setIndexFieldName("issn").setAnalyzerName(DefaultAnalyzers.LC_KEYWORD).build()).build();

		ZuliaIndex.IndexSettings indexSettings = ZuliaIndex.IndexSettings.newBuilder().addFieldConfig(titleConfig).addFieldConfig(issn)
				.addAnalyzerSettings(textAnalyzer).addAnalyzerSettings(entitySettings).build();

		ServerIndexConfig serverIndexConfig = new ServerIndexConfig(indexSettings);
		ZuliaPerFieldAnalyzer zuliaPerFieldAnalyzer = new ZuliaPerFieldAnalyzer(serverIndexConfig);

		ZuliaLegacyMultiFieldQueryParser zuliaQueryParser = new ZuliaLegacyMultiFieldQueryParser(zuliaPerFieldAnalyzer, serverIndexConfig);
		zuliaQueryParser.setSplitOnWhitespace(true);
		zuliaQueryParser.setAutoGeneratePhraseQueries(true);

		zuliaQueryParser.setDefaultOperator(QueryParser.Operator.AND);
		zuliaQueryParser.setDefaultFields(Arrays.asList("title", "issn"));
		Query parse = zuliaQueryParser.parse("issn:Cancer\\ Colour-\\ stuff");

		System.out.println(parse);
	}

}
