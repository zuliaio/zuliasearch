package io.zulia.queryparser.test;

import io.zulia.DefaultAnalyzers;
import io.zulia.message.ZuliaIndex;
import io.zulia.server.analysis.ZuliaPerFieldAnalyzer;
import io.zulia.server.config.ServerIndexConfig;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ZuliaPerFieldAnalyzerTest {

	@Test
	public void refreshAppliesReloadedAnalyzer() throws IOException {
		ServerIndexConfig serverIndexConfig = new ServerIndexConfig(settingsWithTitleAnalyzer(DefaultAnalyzers.STANDARD));
		ZuliaPerFieldAnalyzer analyzer = new ZuliaPerFieldAnalyzer(serverIndexConfig);

		Assertions.assertEquals(List.of("lung", "cancer"), terms(analyzer, "title", "Lung Cancer"));

		// the same thread analyzed title before the reload, so cached components must not survive it
		serverIndexConfig.configure(settingsWithTitleAnalyzer(DefaultAnalyzers.KEYWORD));
		analyzer.refresh();

		Assertions.assertEquals(List.of("Lung Cancer"), terms(analyzer, "title", "Lung Cancer"));
	}

	@Test
	public void unknownFieldUsesKeywordDefault() throws IOException {
		ZuliaPerFieldAnalyzer analyzer = new ZuliaPerFieldAnalyzer(new ServerIndexConfig(settingsWithTitleAnalyzer(DefaultAnalyzers.STANDARD)));
		Assertions.assertEquals(List.of("Lung Cancer"), terms(analyzer, "notAField", "Lung Cancer"));
	}

	private static ZuliaIndex.IndexSettings settingsWithTitleAnalyzer(String analyzerName) {
		return ZuliaIndex.IndexSettings.newBuilder().addFieldConfig(ZuliaIndex.FieldConfig.newBuilder()
				.addIndexAs(ZuliaIndex.IndexAs.newBuilder().setAnalyzerName(analyzerName).setIndexFieldName("title"))
				.setFieldType(ZuliaIndex.FieldConfig.FieldType.STRING).setStoredFieldName("title")).build();
	}

	private static List<String> terms(Analyzer analyzer, String field, String text) throws IOException {
		List<String> terms = new ArrayList<>();
		try (TokenStream tokenStream = analyzer.tokenStream(field, text)) {
			CharTermAttribute termAttribute = tokenStream.addAttribute(CharTermAttribute.class);
			tokenStream.reset();
			while (tokenStream.incrementToken()) {
				terms.add(termAttribute.toString());
			}
			tokenStream.end();
		}
		return terms;
	}
}
