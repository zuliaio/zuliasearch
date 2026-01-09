package io.zulia.server.analysis;

import io.zulia.message.ZuliaIndex;
import io.zulia.message.ZuliaIndex.AnalyzerSettings;
import io.zulia.message.ZuliaIndex.FieldConfig.FieldType;
import io.zulia.server.HtmlStrippingWrapper;
import io.zulia.server.analysis.filter.BritishUSFilter;
import io.zulia.server.analysis.filter.CaseProtectedWordsFilter;
import io.zulia.server.config.IndexFieldInfo;
import io.zulia.server.config.ServerIndexConfig;
import io.zulia.server.field.FieldTypeUtil;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.AnalyzerWrapper;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.DelegatingAnalyzerWrapper;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.charfilter.HTMLStripCharFilter;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.core.UpperCaseFilter;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.de.GermanNormalizationFilter;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.en.EnglishMinimalStemFilter;
import org.apache.lucene.analysis.en.EnglishPossessiveFilter;
import org.apache.lucene.analysis.en.KStemFilter;
import org.apache.lucene.analysis.minhash.MinHashFilterFactory;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;
import org.apache.lucene.analysis.miscellaneous.WordDelimiterGraphFilter;
import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.apache.lucene.analysis.snowball.SnowballFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.lucene.analysis.miscellaneous.WordDelimiterGraphFilter.CATENATE_ALL;

public class ZuliaPerFieldAnalyzer extends DelegatingAnalyzerWrapper {

	private final ServerIndexConfig indexConfig;
	private final Analyzer defaultAnalyzer;

	private Map<String, Analyzer> fieldAnalyzers;

	public ZuliaPerFieldAnalyzer(ServerIndexConfig indexConfig) {
		super(PER_FIELD_REUSE_STRATEGY);
		this.indexConfig = indexConfig;
		this.defaultAnalyzer = new KeywordAnalyzer();
		this.fieldAnalyzers = new HashMap<>();
		refresh();
	}

	public void refresh() {
		Map<String, Analyzer> newFieldAnalyzers = new HashMap<>();
		for (String indexFieldName : indexConfig.getIndexedFields()) {

			IndexFieldInfo indexFieldInfo = indexConfig.getIndexFieldInfo(indexFieldName);
			ZuliaIndex.IndexAs indexAs = indexFieldInfo.getIndexAs();
			AnalyzerSettings analyzerSettings = indexAs != null ? indexConfig.getAnalyzerSettingsByName(indexAs.getAnalyzerName()) : null;
			FieldType fieldType = indexFieldInfo.getFieldType();

			Analyzer a;

			if (FieldTypeUtil.isStringFieldType(fieldType)) {
				if (analyzerSettings != null) {
					a = new ZuliaFieldAnalyzer(analyzerSettings);
					if (analyzerSettings.getStripHTML()) {
						a = new HtmlStrippingWrapper(a);
					}
				}
				else {
					a = new KeywordAnalyzer();
				}
				newFieldAnalyzers.put(FieldTypeUtil.getCharLengthIndexField(indexFieldName), new WhitespaceAnalyzer());
			}
			else if (FieldTypeUtil.isHandledAsNumericFieldType(fieldType)) {
				a = new WhitespaceAnalyzer();
			}
			else {
				a = new KeywordAnalyzer();
			}


			newFieldAnalyzers.put(indexFieldName, a);
			newFieldAnalyzers.put(FieldTypeUtil.getListLengthIndexField(indexFieldName), new WhitespaceAnalyzer());

		}

		this.fieldAnalyzers = newFieldAnalyzers;
	}

	@Override
	protected Analyzer getWrappedAnalyzer(String fieldName) {
		Analyzer analyzer = fieldAnalyzers.get(fieldName);
		return (analyzer != null) ? analyzer : defaultAnalyzer;
	}

	@Override
	public String toString() {
		return "ZuliaPerFieldAnalyzerWrapper(" + fieldAnalyzers + ", default=" + defaultAnalyzer + ")";
	}



}
