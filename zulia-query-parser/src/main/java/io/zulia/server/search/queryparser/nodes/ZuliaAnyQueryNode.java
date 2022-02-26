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
package io.zulia.server.search.queryparser.nodes;

import org.apache.lucene.queryparser.flexible.core.nodes.AndQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.parser.EscapeQuerySyntax;

import java.util.List;

/** A {@link ZuliaAnyQueryNode} represents an ANY operator performed on a list of nodes. */
public class ZuliaAnyQueryNode extends AndQueryNode {

	private int minimumMatchingElements = 0;

	/** @param clauses - the query nodes to be or'ed */
	public ZuliaAnyQueryNode(List<QueryNode> clauses, int minimumMatchingElements) {
		super(clauses);
		this.minimumMatchingElements = minimumMatchingElements;
	}

	public int getMinimumMatchingElements() {
		return this.minimumMatchingElements;
	}

	@Override
	public QueryNode cloneTree() throws CloneNotSupportedException {
		ZuliaAnyQueryNode clone = (ZuliaAnyQueryNode) super.cloneTree();
		clone.minimumMatchingElements = this.minimumMatchingElements;

		return clone;
	}

	@Override
	public String toString() {
		if (getChildren() == null || getChildren().size() == 0)
			return "<zany" + "'  matchelements=" + this.minimumMatchingElements + "/>";
		StringBuilder sb = new StringBuilder();
		sb.append("<zany").append(" matchelements=").append(this.minimumMatchingElements).append('>');
		for (QueryNode clause : getChildren()) {
			sb.append("\n");
			sb.append(clause.toString());
		}
		sb.append("\n</any>");
		return sb.toString();
	}

	@Override
	public CharSequence toQueryString(EscapeQuerySyntax escapeSyntaxParser) {
		String anySTR = "ZANY " + this.minimumMatchingElements;

		StringBuilder sb = new StringBuilder();
		if (getChildren() == null || getChildren().size() == 0) {
			// no childs case
		}
		else {
			String filler = "";
			for (QueryNode clause : getChildren()) {
				sb.append(filler).append(clause.toQueryString(escapeSyntaxParser));
				filler = " ";
			}
		}

		return anySTR;
	}
}
