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
package io.zulia.server.search.queryparser.zulia.processors;

import io.zulia.message.ZuliaIndex;
import io.zulia.server.field.FieldTypeUtil;
import org.apache.lucene.queryparser.flexible.core.config.FieldConfig;
import org.apache.lucene.queryparser.flexible.core.config.QueryConfigHandler;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.MatchNoDocsQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.RangeQueryNode;
import org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessorImpl;
import org.apache.lucene.queryparser.flexible.core.util.StringUtils;
import org.apache.lucene.queryparser.flexible.standard.nodes.TermRangeQueryNode;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class ZuliaDateQueryNodeProcessor extends QueryNodeProcessorImpl {

	public ZuliaDateQueryNodeProcessor() {
		// empty constructor
	}

	@Override
	protected QueryNode postProcessNode(QueryNode node) {

		QueryConfigHandler config = getQueryConfigHandler();

		if (config != null) {

			if (node instanceof FieldQueryNode && !(node.getParent() instanceof RangeQueryNode)) {
				FieldQueryNode fieldQueryNode = (FieldQueryNode) node;
				CharSequence field = fieldQueryNode.getField();
				FieldConfig fieldConfig = config.getFieldConfig(StringUtils.toString(field));
				if (fieldConfig != null) {
					ZuliaIndex.FieldConfig.FieldType zuliaFieldType = fieldConfig.get(ZuliaQueryNodeProcessorPipeline.ZULIA_FIELD_TYPE);
					if (FieldTypeUtil.isDateFieldType(zuliaFieldType)) {
						try {
							String date = fieldQueryNode.getTextAsString();
							if (date != null && !date.isEmpty()) {
								fieldQueryNode.setValue(String.valueOf(getDateAsLong(date)));
							}
						}
						catch (Exception e) {
							return new MatchNoDocsQueryNode();
						}
					}
				}
			}
			else if (node instanceof TermRangeQueryNode) {
				TermRangeQueryNode termRangeNode = (TermRangeQueryNode) node;
				CharSequence field = termRangeNode.getField();

				FieldConfig fieldConfig = config.getFieldConfig(StringUtils.toString(field));

				if (fieldConfig != null) {
					ZuliaIndex.FieldConfig.FieldType zuliaFieldType = fieldConfig.get(ZuliaQueryNodeProcessorPipeline.ZULIA_FIELD_TYPE);

					if (FieldTypeUtil.isDateFieldType(zuliaFieldType)) {

						FieldQueryNode upper = termRangeNode.getUpperBound();
						FieldQueryNode lower = termRangeNode.getLowerBound();

						try {
							String lowerText = lower.getTextAsString();
							if (lowerText != null && !lowerText.isEmpty()) {
								lower.setValue(String.valueOf(getDateAsLong(lowerText)));
							}
						}
						catch (Exception e) {
							return new MatchNoDocsQueryNode();
						}

						try {
							String upperText = upper.getTextAsString();
							if (upperText != null && !upperText.isEmpty()) {
								upper.setValue(String.valueOf(getDateAsLong(upperText)));
							}
						}
						catch (Exception e) {
							return new MatchNoDocsQueryNode();
						}
					}

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

	private static Long getDateAsLong(String dateString) {
		long epochMilli;
		if (dateString.contains(":")) {
			epochMilli = Instant.parse(dateString).toEpochMilli();
		}
		else {
			LocalDate parse = LocalDate.parse(dateString, DateTimeFormatter.ISO_LOCAL_DATE);
			epochMilli = parse.atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();
		}
		return epochMilli;
	}
}
