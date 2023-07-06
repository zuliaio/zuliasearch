package io.zulia.server.search.queryparser.node;

import io.zulia.server.config.IndexFieldInfo;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldableNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNodeImpl;
import org.apache.lucene.search.Query;

import java.util.Objects;

public abstract class ZuliaFieldableQueryNode extends QueryNodeImpl implements FieldableNode {
	private CharSequence field;
	private IndexFieldInfo indexFieldInfo;

	@Override
	public CharSequence getField() {
		return field;
	}

	@Override
	public void setField(CharSequence fieldName) {
		this.field = Objects.requireNonNull(fieldName.toString());
	}

	public abstract Query getQuery();

	public void setIndexFieldInfo(IndexFieldInfo indexFieldInfo) {
		this.indexFieldInfo = indexFieldInfo;
	}

	public IndexFieldInfo getIndexFieldInfo() {
		return indexFieldInfo;
	}
}
