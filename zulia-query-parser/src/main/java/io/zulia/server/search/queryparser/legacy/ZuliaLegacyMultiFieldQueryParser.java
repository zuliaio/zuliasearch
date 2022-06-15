package io.zulia.server.search.queryparser.legacy;

import io.zulia.message.ZuliaQuery;
import io.zulia.server.config.ServerIndexConfig;
import io.zulia.server.search.queryparser.ZuliaParser;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Created by Matt Davis on 5/14/16.
 * @author mdavis
 * Copied mostly from org.apache.lucene.queryparser.classic.MultiFieldQueryParser
 */
@Deprecated
public class ZuliaLegacyMultiFieldQueryParser extends ZuliaLegacyQueryParser implements ZuliaParser {

	protected List<String> fields;
	protected Map<String, Float> boosts;
	private float dismaxTie = 0;
	private boolean dismax = false;

	public ZuliaLegacyMultiFieldQueryParser(Analyzer analyzer, ServerIndexConfig indexConfig) {
		super(analyzer, indexConfig);
	}

	public void enableDismax(float dismaxTie) {
		this.dismaxTie = dismaxTie;
		this.dismax = true;
	}

	public void disableDismax() {
		this.dismax = false;
	}

	public void setDefaultFields(Collection<String> fields) {

		Map<String, Float> boostMap = new HashMap<>();
		Set<String> allFields = new TreeSet<>();
		for (String field : fields) {

			Float boost = null;
			if (field.contains("^")) {
				boost = Float.parseFloat(field.substring(field.indexOf("^") + 1));
				try {
					field = field.substring(0, field.indexOf("^"));

				}
				catch (Exception e) {
					throw new IllegalArgumentException("Invalid queryText field boost <" + field + ">");
				}
			}

			Set<String> fieldNames = indexConfig.getMatchingFields(field);
			allFields.addAll(fieldNames);

			if (boost != null) {
				for (String f : fieldNames) {
					boostMap.put(f, boost);
				}
			}

		}

		super.setDefaultField(null);
		this.fields = new ArrayList<>(allFields);
		this.boosts = boostMap;
	}

	@Override
	public void setDefaultOperator(ZuliaQuery.Query.Operator defaultOperator) {
		QueryParser.Operator operator = null;
		if (defaultOperator.equals(ZuliaQuery.Query.Operator.OR)) {
			operator = QueryParser.Operator.OR;
		}
		else if (defaultOperator.equals(ZuliaQuery.Query.Operator.AND)) {
			operator = QueryParser.Operator.AND;
		}
		else {
			//this should never happen
			throw new IllegalArgumentException("Unknown operator type: <" + defaultOperator + ">");
		}
		setDefaultOperator(operator);
	}

	@Override
	public void setDefaultField(String field) {
		super.setDefaultField(field);
		this.fields = null;
	}

	@Override
	protected Query getFieldQuery(String field, String queryText, int slop) throws ParseException {
		if (field == null) {
			List<Query> clauses = new ArrayList<>();
			for (String f : fields) {
				Query q = super.getFieldQuery(f, queryText, true);
				if (q != null) {
					//If the user passes a map of boosts
					if (boosts != null) {
						//Get the boost from the map and apply them
						Float boost = boosts.get(f);
						if (boost != null) {
							q = new BoostQuery(q, boost);
						}
					}
					q = applySlop(q, slop);
					clauses.add(q);
				}
			}
			if (clauses.size() == 0)  // happens for stopwords
				return null;
			return getMultiFieldQuery(clauses);
		}
		Query q = super.getFieldQuery(field, queryText, true);
		q = applySlop(q, slop);
		return q;
	}

	private Query applySlop(Query q, int slop) {
		if (q instanceof PhraseQuery) {
			PhraseQuery.Builder builder = new PhraseQuery.Builder();
			builder.setSlop(slop);
			PhraseQuery pq = (PhraseQuery) q;
			org.apache.lucene.index.Term[] terms = pq.getTerms();
			int[] positions = pq.getPositions();
			for (int i = 0; i < terms.length; ++i) {
				builder.add(terms[i], positions[i]);
			}
			q = builder.build();
		}
		else if (q instanceof MultiPhraseQuery) {
			MultiPhraseQuery mpq = (MultiPhraseQuery) q;

			if (slop != mpq.getSlop()) {
				q = new MultiPhraseQuery.Builder(mpq).setSlop(slop).build();
			}
		}
		return q;
	}

	@Override
	protected Query getFieldQuery(String field, String queryText, boolean quoted) throws ParseException {
		if (field == null) {
			List<Query> clauses = new ArrayList<>();
			Query[] fieldQueries = new Query[fields.size()];
			int maxTerms = 0;
			for (int i = 0; i < fields.size(); i++) {
				Query q = super.getFieldQuery(fields.get(i), queryText, quoted);
				if (q != null) {
					if (q instanceof BooleanQuery) {
						maxTerms = Math.max(maxTerms, ((BooleanQuery) q).clauses().size());
					}
					else {
						maxTerms = Math.max(1, maxTerms);
					}
					fieldQueries[i] = q;
				}
			}
			for (int termNum = 0; termNum < maxTerms; termNum++) {
				List<Query> termClauses = new ArrayList<>();
				for (int i = 0; i < fields.size(); i++) {
					if (fieldQueries[i] != null) {
						Query q = null;
						if (fieldQueries[i] instanceof BooleanQuery) {
							List<BooleanClause> nestedClauses = ((BooleanQuery) fieldQueries[i]).clauses();
							if (termNum < nestedClauses.size()) {
								q = nestedClauses.get(termNum).getQuery();
							}
						}
						else if (termNum == 0) { // e.g. TermQuery-s
							q = fieldQueries[i];
						}
						if (q != null) {
							if (boosts != null) {
								//Get the boost from the map and apply them
								Float boost = boosts.get(fields.get(i));
								if (boost != null) {
									q = new BoostQuery(q, boost);
								}
							}
							termClauses.add(q);
						}
					}
				}
				if (maxTerms > 1) {
					if (termClauses.size() > 0) {
						//mdavis - don't use super method because of min match
						BooleanQuery.Builder builder = new BooleanQuery.Builder();
						for (Query termClause : termClauses) {
							builder.add(termClause, BooleanClause.Occur.SHOULD);
						}
						clauses.add(builder.build());
					}
				}
				else {
					clauses.addAll(termClauses);
				}
			}
			if (clauses.size() == 0)  // happens for stopwords
				return null;
			return getMultiFieldQuery(clauses);
		}

		//mdavis
		field = ZuliaParser.rewriteLengthFields(field);

		return super.getFieldQuery(field, queryText, quoted);
	}

	@Override
	protected Query getFuzzyQuery(String field, String termStr, float minSimilarity) throws ParseException {
		if (field == null) {
			List<Query> clauses = new ArrayList<>();
			for (String f : fields) {
				clauses.add(getFuzzyQuery(f, termStr, minSimilarity));
			}
			return getMultiFieldQuery(clauses);
		}
		return super.getFuzzyQuery(field, termStr, minSimilarity);
	}

	@Override
	protected Query getPrefixQuery(String field, String termStr) throws ParseException {
		if (field == null) {
			List<Query> clauses = new ArrayList<>();
			for (String f : fields) {
				clauses.add(getPrefixQuery(f, termStr));
			}
			return getMultiFieldQuery(clauses);
		}
		return super.getPrefixQuery(field, termStr);
	}

	@Override
	protected Query getWildcardQuery(String field, String termStr) throws ParseException {
		if (field == null) {
			List<Query> clauses = new ArrayList<>();
			for (String f : fields) {
				clauses.add(getWildcardQuery(f, termStr));
			}
			return getMultiFieldQuery(clauses);
		}
		return super.getWildcardQuery(field, termStr);
	}

	@Override
	protected Query getRangeQuery(String field, String part1, String part2, boolean startInclusive, boolean endInclusive) throws ParseException {
		if (field == null) {
			List<Query> clauses = new ArrayList<>();
			for (String f : fields) {
				clauses.add(getRangeQuery(f, part1, part2, startInclusive, endInclusive));
			}
			return getMultiFieldQuery(clauses);
		}
		return super.getRangeQuery(field, part1, part2, startInclusive, endInclusive);
	}

	@Override
	protected Query getRegexpQuery(String field, String termStr) throws ParseException {
		if (field == null) {
			List<Query> clauses = new ArrayList<>();
			for (String f : fields) {
				clauses.add(getRegexpQuery(f, termStr));
			}
			return getMultiFieldQuery(clauses);
		}
		return super.getRegexpQuery(field, termStr);
	}

	/** Creates a multi-field query */
	// TODO: investigate more general approach by default, e.g. DisjunctionMaxQuery?
	protected Query getMultiFieldQuery(List<Query> queries) throws ParseException {
		if (queries.isEmpty()) {
			return null; // all clause words were filtered away by the analyzer.
		}

		if (dismax) {
			return new DisjunctionMaxQuery(queries, dismaxTie);
		}
		else {
			//mdavis - don't use super method because of min match
			BooleanQuery.Builder query = new BooleanQuery.Builder();
			for (Query sub : queries) {
				query.add(sub, BooleanClause.Occur.SHOULD);
			}

			return query.build();
		}
	}

}
