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
package io.zulia.server.search.queryparser.processors;

import io.zulia.server.config.ServerIndexConfig;
import io.zulia.server.search.queryparser.ZuliaParser;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.config.QueryConfigHandler;
import org.apache.lucene.queryparser.flexible.core.nodes.BooleanQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldableNode;
import org.apache.lucene.queryparser.flexible.core.nodes.GroupQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.MatchNoDocsQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.OrQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessorImpl;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler.ConfigurationKeys;

import java.util.ArrayList;
import java.util.List;

/**
 * This processor is used to expand terms so the query looks for the same term in different fields.
 * It also boosts a query based on its field. <br>
 * <br>
 * This processor looks for every {@link FieldableNode} contained in the query node tree. If a
 * {@link FieldableNode} is found, it checks if there is a {@link ConfigurationKeys#MULTI_FIELDS}
 * defined in the {@link QueryConfigHandler}. If there is, the {@link FieldableNode} is cloned N
 * times and the clones are added to a {@link BooleanQueryNode} together with the original node. N
 * is defined by the number of fields that it will be expanded to. The {@link BooleanQueryNode} is
 * returned.
 *
 * @see ConfigurationKeys#MULTI_FIELDS
 */
public class ZuliaMultiFieldQueryNodeProcessor extends QueryNodeProcessorImpl {

	private boolean processChildren = true;

	public ZuliaMultiFieldQueryNodeProcessor() {
		// empty constructor
	}

	@Override
	protected QueryNode postProcessNode(QueryNode node) throws QueryNodeException {

		return node;
	}

	@Override
	protected void processChildren(QueryNode queryTree) throws QueryNodeException {

		if (this.processChildren) {
			super.processChildren(queryTree);

		}
		else {
			this.processChildren = true;
		}
	}

	@Override
	protected QueryNode preProcessNode(QueryNode node) throws QueryNodeException {

		if (node instanceof FieldableNode fieldNode) {

			ServerIndexConfig serverIndexConfig = getQueryConfigHandler().get(ZuliaQueryNodeProcessorPipeline.ZULIA_INDEX_CONFIG);

			this.processChildren = false;

			if (fieldNode.getField() != null) {

				String fieldsStr = fieldNode.getField().toString();

				boolean matchAll = false;
				if (node instanceof FieldQueryNode fqn) {
					matchAll = "*".equals(fqn.getField().toString()) && "*".equals(fqn.getTextAsString());
				}

				List<String> fields = matchAll ? List.of(fieldsStr) : ZuliaParser.expandFields(serverIndexConfig, fieldsStr);

				fieldNode.setField(ZuliaParser.rewriteLengthFields(fields.get(0)));

				if (fields.size() == 1) {
					return fieldNode;
				}
				else {
					return getGroupQueryNode(fieldNode, fields);
				}
			}
			else if (fieldNode.getField() == null) {

				List<String> fields = ZuliaParser.expandFields(serverIndexConfig, getQueryConfigHandler().get(ConfigurationKeys.MULTI_FIELDS));

				if (fields == null) {
					throw new IllegalArgumentException("StandardQueryConfigHandler.ConfigurationKeys.MULTI_FIELDS should be set on the QueryConfigHandler");
				}

				if (fields.size() > 0) {
					fieldNode.setField(ZuliaParser.rewriteLengthFields(fields.get(0)));

					if (fields.size() == 1) {
						return fieldNode;
					}
					else {
						return getGroupQueryNode(fieldNode, fields);
					}
				}

			}

			return new MatchNoDocsQueryNode();
		}
		return node;
	}

	private QueryNode getGroupQueryNode(FieldableNode fieldNode, List<String> fields) {
		List<QueryNode> children = new ArrayList<>(fields.size());

		children.add(fieldNode);
		for (int i = 1; i < fields.size(); i++) {
			try {
				fieldNode = (FieldableNode) fieldNode.cloneTree();
				fieldNode.setField(ZuliaParser.rewriteLengthFields(fields.get(i)));
				children.add(fieldNode);
			}
			catch (CloneNotSupportedException e) {
				throw new RuntimeException(e);
			}
		}

		return new GroupQueryNode(new OrQueryNode(children));
	}

	@Override
	protected List<QueryNode> setChildrenOrder(List<QueryNode> children) {
		return children;
	}

}
