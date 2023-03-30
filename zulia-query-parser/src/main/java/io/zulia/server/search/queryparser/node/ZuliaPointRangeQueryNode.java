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
		StringBuilder sb = new StringBuilder();
		sb.append("<zuliaPointRange lowerInclusive='");
		sb.append(isLowerInclusive());
		sb.append("' upperInclusive='");
		sb.append(isUpperInclusive());
		sb.append("' indexFieldInfo.storedFieldName='");
		sb.append(indexFieldInfo.getStoredFieldName());
		sb.append("' indexFieldInfo.internalFieldName='");
		sb.append(indexFieldInfo.getInternalFieldName());
		sb.append("' indexFieldInfo.internalSortFieldName='");
		sb.append(indexFieldInfo.getInternalSortFieldName());
		sb.append("' indexFieldInfo.fieldType='");
		sb.append(indexFieldInfo.getFieldType());
		sb.append("' indexFieldInfo.indexAs.analyzerName='");
		sb.append(indexFieldInfo.getIndexAs().getAnalyzerName());
		sb.append("' indexFieldInfo.indexAs.indexFieldName='");
		sb.append(indexFieldInfo.getIndexAs().getIndexFieldName());
		sb.append("'>\n");
		sb.append(getLowerBound()).append('\n');
		sb.append(getUpperBound()).append('\n');
		sb.append("</zuliaPointRange>");
		return sb.toString();
	}
}
