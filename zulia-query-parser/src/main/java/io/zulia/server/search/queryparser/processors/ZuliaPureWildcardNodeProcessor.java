package io.zulia.server.search.queryparser.processors;

import io.zulia.ZuliaConstants;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.MatchAllDocsQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessorImpl;
import org.apache.lucene.queryparser.flexible.standard.nodes.WildcardQueryNode;

import java.util.List;

public class ZuliaPureWildcardNodeProcessor extends QueryNodeProcessorImpl {

    /**
     * Constructs a {@link ZuliaPureWildcardNodeProcessor} object.
     */
    public ZuliaPureWildcardNodeProcessor() {
        // empty constructor
    }

    @Override
    protected QueryNode postProcessNode(QueryNode node) {

        if (node instanceof WildcardQueryNode wildcardQueryNode) {
            String field = wildcardQueryNode.getFieldAsString();
            String text = wildcardQueryNode.getTextAsString();

            if ("*".equals(text)) {
                if ("*".equals(field)) {
                    return new MatchAllDocsQueryNode();
                } else {
                    return new FieldQueryNode(ZuliaConstants.FIELDS_LIST_FIELD, field, wildcardQueryNode.getBegin(), wildcardQueryNode.getEnd());
                }
            }

        }
        return node;
    }

    @Override
    protected QueryNode preProcessNode(QueryNode node) {
        return node;
    }

    @Override
    protected List<QueryNode> setChildrenOrder(List<QueryNode> children) {
        return children;
    }
}
