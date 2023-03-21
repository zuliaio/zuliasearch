package io.zulia.server.search.queryparser.node;

import io.zulia.message.ZuliaIndex.FieldConfig.FieldType;
import org.apache.lucene.queryparser.flexible.standard.nodes.AbstractRangeQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.PointQueryNode;

public class ZuliaPointRangeQueryNode extends AbstractRangeQueryNode<PointQueryNode> {

	public FieldType fieldType;
	private String sortField;

	public ZuliaPointRangeQueryNode(PointQueryNode lower, PointQueryNode upper, boolean lowerInclusive, boolean upperInclusive, FieldType fieldType,
			String sortField) {
		setBounds(lower, upper, lowerInclusive, upperInclusive, fieldType, sortField);
	}

	public void setBounds(PointQueryNode lower, PointQueryNode upper, boolean lowerInclusive, boolean upperInclusive, FieldType fieldType, String sortField) {

		super.setBounds(lower, upper, lowerInclusive, upperInclusive);
		this.fieldType = fieldType;
		this.sortField = sortField;
	}

	public FieldType getFieldConfig() {
		return this.fieldType;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("<zuliaPointRange lowerInclusive='");
		sb.append(isLowerInclusive());
		sb.append("' upperInclusive='");
		sb.append(isUpperInclusive());
		sb.append("' fieldType='");
		sb.append(fieldType);
		sb.append("' sortField='");
		sb.append(sortField);
		sb.append("'>\n");
		sb.append(getLowerBound()).append('\n');
		sb.append(getUpperBound()).append('\n');
		sb.append("</zuliaPointRange>");
		return sb.toString();
	}
}
