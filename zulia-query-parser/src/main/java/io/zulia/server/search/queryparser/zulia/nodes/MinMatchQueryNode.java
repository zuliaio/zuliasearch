/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zulia.server.search.queryparser.zulia.nodes;

import org.apache.lucene.queryparser.flexible.core.QueryNodeError;
import org.apache.lucene.queryparser.flexible.core.messages.QueryParserMessages;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNodeImpl;
import org.apache.lucene.queryparser.flexible.core.parser.EscapeQuerySyntax;
import org.apache.lucene.queryparser.flexible.messages.MessageImpl;

import java.util.List;

/**
 * A {@link MinMatchQueryNode} changes a boolean query the QueryNode tree which is under this node
 * to contain minimum should match. So, it must only and always have one child.
 */
public class MinMatchQueryNode extends QueryNodeImpl {

	private int value = 0;

	/**
	 * Constructs a min match query node
	 *
	 * @param query the query to have minimum should match applied
	 * @param value the minimum match value
	 */
	public MinMatchQueryNode(QueryNode query, int value) {
		if (query == null) {
			throw new QueryNodeError(new MessageImpl(QueryParserMessages.NODE_ACTION_NOT_SUPPORTED, "query", "null"));
		}

		this.value = value;
		setLeaf(false);
		allocate();
		add(query);
	}

	/**
	 * Returns the single child which this node minimum should match.
	 *
	 * @return the single child which this node minimum should matchs
	 */
	public QueryNode getChild() {
		List<QueryNode> children = getChildren();

		if (children == null || children.size() == 0) {
			return null;
		}

		return children.get(0);
	}

	/**
	 * Returns the minimum should match value
	 *
	 * @return the minimum should match value
	 */
	public int getValue() {
		return this.value;
	}

	/**
	 * Returns the minimum should match value parsed to a string.
	 *
	 * @return the parsed value
	 */
	private CharSequence getValueString() {
		return "" + value;
	}

	@Override
	public String toString() {
		return "<minmatch value='" + getValueString() + "'>" + "\n" + getChild().toString() + "\n</minmatch>";
	}

	@Override
	public CharSequence toQueryString(EscapeQuerySyntax escapeSyntaxParser) {
		if (getChild() == null)
			return "";
		return getChild().toQueryString(escapeSyntaxParser) + "~" + getValueString();
	}

	@Override
	public QueryNode cloneTree() throws CloneNotSupportedException {
		MinMatchQueryNode clone = (MinMatchQueryNode) super.cloneTree();

		clone.value = this.value;

		return clone;
	}
}
