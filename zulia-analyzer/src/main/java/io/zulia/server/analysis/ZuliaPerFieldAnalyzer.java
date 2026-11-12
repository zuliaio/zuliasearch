package io.zulia.server.analysis;

import io.zulia.message.ZuliaIndex;
import io.zulia.message.ZuliaIndex.AnalyzerSettings;
import io.zulia.message.ZuliaIndex.FieldConfig.FieldType;
import io.zulia.server.HtmlStrippingWrapper;
import io.zulia.server.config.IndexFieldInfo;
import io.zulia.server.config.ServerIndexConfig;
import io.zulia.server.field.FieldTypeUtil;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.DelegatingAnalyzerWrapper;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;

import java.util.HashMap;
import java.util.Map;

public class ZuliaPerFieldAnalyzer extends DelegatingAnalyzerWrapper {

	private final ServerIndexConfig indexConfig;
	private final Analyzer defaultAnalyzer;

	// volatile because refresh() swaps the map during a live settings reload while query and indexing threads read it
	private volatile Map<String, Analyzer> fieldAnalyzers;

	public ZuliaPerFieldAnalyzer(ServerIndexConfig indexConfig) {
		super(PER_FIELD_REUSE_STRATEGY);
		this.indexConfig = indexConfig;
		this.defaultAnalyzer = new KeywordAnalyzer();
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
