package io.zulia.server.search.queryparser.processors;

import io.zulia.message.ZuliaIndex.FieldConfig.FieldType;
import io.zulia.server.config.ServerIndexConfig;
import org.apache.lucene.queryparser.flexible.core.config.ConfigurationKey;
import org.apache.lucene.queryparser.flexible.core.config.QueryConfigHandler;
import org.apache.lucene.queryparser.flexible.core.processors.NoChildOptimizationQueryNodeProcessor;
import org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessorPipeline;
import org.apache.lucene.queryparser.flexible.core.processors.RemoveDeletedQueryNodesProcessor;
import org.apache.lucene.queryparser.flexible.standard.processors.AllowLeadingWildcardProcessor;
import org.apache.lucene.queryparser.flexible.standard.processors.AnalyzerQueryNodeProcessor;
import org.apache.lucene.queryparser.flexible.standard.processors.BooleanQuery2ModifierNodeProcessor;
import org.apache.lucene.queryparser.flexible.standard.processors.BooleanSingleChildOptimizationQueryNodeProcessor;
import org.apache.lucene.queryparser.flexible.standard.processors.BoostQueryNodeProcessor;
import org.apache.lucene.queryparser.flexible.standard.processors.DefaultPhraseSlopQueryNodeProcessor;
import org.apache.lucene.queryparser.flexible.standard.processors.FuzzyQueryNodeProcessor;
import org.apache.lucene.queryparser.flexible.standard.processors.IntervalQueryNodeProcessor;
import org.apache.lucene.queryparser.flexible.standard.processors.MultiTermRewriteMethodProcessor;
import org.apache.lucene.queryparser.flexible.standard.processors.OpenRangeQueryNodeProcessor;
import org.apache.lucene.queryparser.flexible.standard.processors.PhraseSlopQueryNodeProcessor;
import org.apache.lucene.queryparser.flexible.standard.processors.RegexpQueryNodeProcessor;
import org.apache.lucene.queryparser.flexible.standard.processors.RemoveEmptyNonLeafQueryNodeProcessor;
import org.apache.lucene.queryparser.flexible.standard.processors.WildcardQueryNodeProcessor;

//Copied from org.apache.lucene.queryparser.flexible.standard.processors.StandardQueryNodeProcessorPipeline within changes noted below as // zulia -

public class ZuliaQueryNodeProcessorPipeline extends QueryNodeProcessorPipeline {

	public final static ConfigurationKey<Integer> GLOBAL_MM = ConfigurationKey.newInstance();

	public static final ConfigurationKey<FieldType> ZULIA_FIELD_TYPE = ConfigurationKey.newInstance();

	public static final ConfigurationKey<ServerIndexConfig> ZULIA_INDEX_CONFIG = ConfigurationKey.newInstance();

	public ZuliaQueryNodeProcessorPipeline(QueryConfigHandler queryConfig) {
		super(queryConfig);

		//zulia - add global min match handler
		add(new ZuliaGlobalMinMatchProcessor());

		//zulia - change MultiFieldQueryNodeProcessor to ZuliaMultiFieldQueryNodeProcessor
		add(new ZuliaMultiFieldQueryNodeProcessor());
		add(new WildcardQueryNodeProcessor());
		add(new ZuliaPureWildcardNodeProcessor());
		add(new FuzzyQueryNodeProcessor());
		add(new RegexpQueryNodeProcessor());

		//handled by ZuliaPureWildcardNodeProcessor
		//add(new MatchAllDocsQueryNodeProcessor());
		add(new OpenRangeQueryNodeProcessor());
		//zulia - add ZuliaPointQueryNodeProcessor comment PointRangeQueryNodeProcessor
		add(new ZuliaPointQueryNodeProcessor());
		//add(new PointRangeQueryNodeProcessor());
		//zulia - remove term range query parser, replaced by ZuliaDateRangeQueryNodeProcessor above
		//add(new TermRangeQueryNodeProcessor());
		add(new AllowLeadingWildcardProcessor());
		add(new AnalyzerQueryNodeProcessor());
		add(new PhraseSlopQueryNodeProcessor());
		add(new BooleanQuery2ModifierNodeProcessor());
		add(new NoChildOptimizationQueryNodeProcessor());
		add(new RemoveDeletedQueryNodesProcessor());
		add(new RemoveEmptyNonLeafQueryNodeProcessor());
		add(new BooleanSingleChildOptimizationQueryNodeProcessor());
		add(new DefaultPhraseSlopQueryNodeProcessor());
		add(new BoostQueryNodeProcessor());
		add(new MultiTermRewriteMethodProcessor());
		add(new IntervalQueryNodeProcessor());

	}
}
