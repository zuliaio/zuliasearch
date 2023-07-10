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

public class ZuliaTermsInSetQueryNode extends ZuliaFieldableQueryNode {
	private final List<CharSequence> terms;

	public ZuliaTermsInSetQueryNode(CharSequence field, List<CharSequence> terms) {
		setField(field);
		this.terms = Objects.requireNonNull(terms);
	}

	public Query getQuery() {
		Objects.requireNonNull(getField(), "Field must not be null for term set queries.");
		Objects.requireNonNull(getIndexFieldInfo(), "Field <" + getField() + "> must be indexed for term set queries");
		return SetQueryHelper.getTermInSetQuery(terms.stream().map(Object::toString).toList(), getField().toString(), getIndexFieldInfo());
	}

	@Override
	public String toQueryString(EscapeQuerySyntax escapeSyntaxParser) {
		return String.format(Locale.ROOT, "%s:termQuery(%s)", getField(), terms);
	}

	@Override
	public String toString() {
		return toQueryString(new EscapeQuerySyntaxImpl());
	}

	@Override
	public ZuliaTermsInSetQueryNode cloneTree() {
		return new ZuliaTermsInSetQueryNode(getField(), terms);
	}

}
