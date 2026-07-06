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
package io.zulia.server.search.queryparser.node;

import io.zulia.server.search.queryparser.SetQueryHelper;
import org.apache.lucene.queryparser.flexible.core.parser.EscapeQuerySyntax;
import org.apache.lucene.queryparser.flexible.standard.parser.EscapeQuerySyntaxImpl;
import org.apache.lucene.search.Query;

import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

public class ZuliaNumericSetQueryNode extends ZuliaFieldableQueryNode {
	private final List<CharSequence> terms;

	public ZuliaNumericSetQueryNode(CharSequence field, List<CharSequence> terms) {
		setField(field);
		this.terms = Objects.requireNonNull(terms);
	}

	public Query getQuery() {
		Objects.requireNonNull(getField(), "Field must not be null for numeric set queries");
		Objects.requireNonNull(getIndexFieldInfo(), "Field " + getField() + " must be indexed for numeric set queries");
		return SetQueryHelper.getNumericSetQuery(getField().toString(), getIndexFieldInfo(), intTerms(), longTerms(), floatTerms(), doubleTerms());
	}

	public Supplier<List<Integer>> intTerms() {
		return () -> parseTerms(Integer::parseInt, "int");
	}

	public Supplier<List<Long>> longTerms() {
		return () -> parseTerms(Long::parseLong, "long");
	}

	public Supplier<List<Float>> floatTerms() {
		return () -> parseTerms(Float::parseFloat, "float");
	}

	public Supplier<List<Double>> doubleTerms() {
		return () -> parseTerms(Double::parseDouble, "double");
	}

	private <T> List<T> parseTerms(Function<String, T> parser, String typeName) {
		return terms.stream().map(Object::toString).map(term -> {
			try {
				return parser.apply(term);
			}
			catch (NumberFormatException e) {
				throw new IllegalArgumentException(
						"Invalid value <" + term + "> for numeric set query on " + typeName + " field <" + getField() + ">. Every value must be a valid "
								+ typeName);
			}
		}).toList();
	}

	@Override
	public String toQueryString(EscapeQuerySyntax escapeSyntaxParser) {
		return String.format(Locale.ROOT, "%s:numericSet(%s)", getField(), terms);
	}

	@Override
	public String toString() {
		return toQueryString(new EscapeQuerySyntaxImpl());
	}

	@Override
	public ZuliaNumericSetQueryNode cloneTree() {
		return new ZuliaNumericSetQueryNode(getField(), terms);
	}

}
