package io.zulia.server.search.queryparser.zulia.processors;

import org.apache.lucene.queryparser.flexible.core.config.QueryConfigHandler;
import org.apache.lucene.queryparser.flexible.core.processors.NoChildOptimizationQueryNodeProcessor;
import org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessorPipeline;
import org.apache.lucene.queryparser.flexible.core.processors.RemoveDeletedQueryNodesProcessor;
import org.apache.lucene.queryparser.flexible.standard.processors.*;

//Copied from org.apache.lucene.queryparser.flexible.standard.processors.StandardQueryNodeProcessorPipeline within changes noted below as // zulia -

public class ZuliaQueryNodeProcessorPipeline extends QueryNodeProcessorPipeline {

  public ZuliaQueryNodeProcessorPipeline(QueryConfigHandler queryConfig) {
    super(queryConfig);

    add(new WildcardQueryNodeProcessor());
    //zulia - change MultiFieldQueryNodeProcessor to ZuliaMultiFieldQueryNodeProcessor
    add(new ZuliaMultiFieldQueryNodeProcessor());
    add(new FuzzyQueryNodeProcessor());
    add(new RegexpQueryNodeProcessor());
    add(new MatchAllDocsQueryNodeProcessor());
    add(new OpenRangeQueryNodeProcessor());
    add(new PointQueryNodeProcessor());
    add(new PointRangeQueryNodeProcessor());
    add(new TermRangeQueryNodeProcessor());
    add(new AllowLeadingWildcardProcessor());
    add(new AnalyzerQueryNodeProcessor());
    add(new PhraseSlopQueryNodeProcessor());
    // add(new GroupQueryNodeProcessor());
    add(new BooleanQuery2ModifierNodeProcessor());
    add(new NoChildOptimizationQueryNodeProcessor());
    add(new RemoveDeletedQueryNodesProcessor());
    add(new RemoveEmptyNonLeafQueryNodeProcessor());
    add(new BooleanSingleChildOptimizationQueryNodeProcessor());
    add(new DefaultPhraseSlopQueryNodeProcessor());
    add(new BoostQueryNodeProcessor());
    add(new MultiTermRewriteMethodProcessor());
  }
}
