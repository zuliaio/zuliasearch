package io.zulia.server.search.queryparser.node;

import io.zulia.server.config.IndexFieldInfo;
import org.apache.lucene.queryparser.flexible.standard.nodes.AbstractRangeQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.PointQueryNode;

public class ZuliaPointRangeQueryNode extends AbstractRangeQueryNode<PointQueryNode> {

	private IndexFieldInfo indexFieldInfo;

	public ZuliaPointRangeQueryNode(PointQueryNode lower, PointQueryNode upper, boolean lowerInclusive, boolean upperInclusive, IndexFieldInfo indexFieldInfo) {
		setBounds(lower, upper, lowerInclusive, upperInclusive, indexFieldInfo);
	}

	public void setBounds(PointQueryNode lower, PointQueryNode upper, boolean lowerInclusive, boolean upperInclusive, IndexFieldInfo indexFieldInfo) {

		super.setBounds(lower, upper, lowerInclusive, upperInclusive);
		this.indexFieldInfo = indexFieldInfo;

	}

	public IndexFieldInfo getIndexFieldMapping() {
		return indexFieldInfo;
	}

	@Override
	public String toString() {
		return "<zuliaPointRange lowerInclusive='" + isLowerInclusive() + "' upperInclusive='" + isUpperInclusive() + "' indexFieldInfo.storedFieldName='"
				+ indexFieldInfo.getStoredFieldName() + "' indexFieldInfo.internalFieldName='" + indexFieldInfo.getInternalFieldName()
				+ "' indexFieldInfo.internalSortFieldName='" + indexFieldInfo.getInternalSortFieldName() + "' indexFieldInfo.fieldType='"
				+ indexFieldInfo.getFieldType() + "' indexFieldInfo.indexAs.analyzerName='" + indexFieldInfo.getIndexAs().getAnalyzerName()
				+ "' indexFieldInfo.indexAs.indexFieldName='" + indexFieldInfo.getIndexAs().getIndexFieldName() + "'>\n" + getLowerBound() + '\n'
				+ getUpperBound() + '\n' + "</zuliaPointRange>";
	}
}
