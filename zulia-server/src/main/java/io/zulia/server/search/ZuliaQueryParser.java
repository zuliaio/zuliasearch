package io.zulia.server.search;

import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import io.zulia.ZuliaConstants;
import io.zulia.message.ZuliaIndex.FieldConfig;
import io.zulia.server.config.ServerIndexConfig;
import io.zulia.server.index.field.FieldTypeUtil;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class ZuliaQueryParser extends QueryParser {

	protected ServerIndexConfig indexConfig;

	private int minimumNumberShouldMatch;

	public ZuliaQueryParser(Analyzer analyzer, ServerIndexConfig indexConfig) {
		super(null, analyzer);
		this.indexConfig = indexConfig;
		setAllowLeadingWildcard(true);
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

	public void setDefaultField(String field) {
		this.field = field;
	}

	public void setMinimumNumberShouldMatch(int minimumNumberShouldMatch) {
		this.minimumNumberShouldMatch = minimumNumberShouldMatch;
	}

	@Override
	protected Query getRangeQuery(String field, String start, String end, boolean startInclusive, boolean endInclusive) throws ParseException {

		FieldConfig.FieldType fieldType = indexConfig.getFieldTypeForIndexField(field);
		if (FieldTypeUtil.isNumericOrDateFieldType(fieldType)) {
			return getNumericOrDateRange(field, start, end, startInclusive, endInclusive);
		}

		return super.getRangeQuery(field, start, end, startInclusive, endInclusive);

	}

	private Query getNumericOrDateRange(final String fieldName, final String start, final String end, final boolean startInclusive,
			final boolean endInclusive) {
		FieldConfig.FieldType fieldType = indexConfig.getFieldTypeForIndexField(fieldName);
		if (FieldTypeUtil.isNumericIntFieldType(fieldType)) {
			int min = start == null ? Integer.MIN_VALUE : Integer.parseInt(start);
			int max = end == null ? Integer.MAX_VALUE : Integer.parseInt(end);
			if (!startInclusive) {
				min = Math.addExact(min, 1);
			}
			if (!endInclusive) {
				max = Math.addExact(max, -1);
			}
			return IntPoint.newRangeQuery(fieldName, min, max);
		}
		else if (FieldTypeUtil.isNumericLongFieldType(fieldType)) {
			long min = start == null ? Long.MIN_VALUE : Long.parseLong(start);
			long max = end == null ? Long.MAX_VALUE : Long.parseLong(end);
			if (!startInclusive) {
				min = Math.addExact(min, 1);
			}
			if (!endInclusive) {
				max = Math.addExact(max, -1);
			}
			return LongPoint.newRangeQuery(fieldName, min, max);
		}
		else if (FieldTypeUtil.isNumericFloatFieldType(fieldType)) {
			float min = start == null ? Float.NEGATIVE_INFINITY : Float.parseFloat(start);
			float max = end == null ? Float.POSITIVE_INFINITY : Float.parseFloat(end);
			if (!startInclusive) {
				min = Math.nextUp(min);
			}
			if (!endInclusive) {
				max = Math.nextDown(max);
			}
			return FloatPoint.newRangeQuery(fieldName, min, max);
		}
		else if (FieldTypeUtil.isNumericDoubleFieldType(fieldType)) {
			double min = start == null ? Double.NEGATIVE_INFINITY : Double.parseDouble(start);
			double max = end == null ? Double.POSITIVE_INFINITY : Double.parseDouble(end);
			if (!startInclusive) {
				min = Math.nextUp(min);
			}
			if (!endInclusive) {
				max = Math.nextDown(max);
			}
			return DoublePoint.newRangeQuery(fieldName, min, max);
		}
		else if (FieldTypeUtil.isDateFieldType(fieldType)) {
			long min = Long.MIN_VALUE;
			long max = Long.MAX_VALUE;
			if (start != null) {
				min = getDateAsLong(start);
			}
			if (end != null) {
				max = getDateAsLong(end);
			}
			if (!startInclusive) {
				min = Math.addExact(min, 1);
			}
			if (!endInclusive) {
				max = Math.addExact(max, 1);
			}
			return LongPoint.newRangeQuery(fieldName, min, max);
		}
		throw new RuntimeException("Not a valid numeric field <" + fieldName + ">");
	}

	@Override
	protected Query newTermQuery(Term term) {
		String field = term.field();
		String text = term.text();

		FieldConfig.FieldType fieldType = indexConfig.getFieldTypeForIndexField(field);
		if (FieldTypeUtil.isNumericOrDateFieldType(fieldType)) {
			if (FieldTypeUtil.isDateFieldType(fieldType)) {
				return getNumericOrDateRange(field, text, text, true, true);
			}
			else {
				if (FieldTypeUtil.isNumericIntFieldType(fieldType) && Ints.tryParse(text) != null) {
					return getNumericOrDateRange(field, text, text, true, true);
				}
				else if (FieldTypeUtil.isNumericLongFieldType(fieldType) && Longs.tryParse(text) != null) {
					return getNumericOrDateRange(field, text, text, true, true);
				}
				else if (FieldTypeUtil.isNumericFloatFieldType(fieldType) && Floats.tryParse(text) != null) {
					return getNumericOrDateRange(field, text, text, true, true);
				}
				else if (FieldTypeUtil.isNumericDoubleFieldType(fieldType) && Doubles.tryParse(text) != null) {
					return getNumericOrDateRange(field, text, text, true, true);
				}
			}
			return new MatchNoDocsQuery(field + " expects numeric");
		}

		return super.newTermQuery(term);
	}

	@Override
	protected BooleanQuery.Builder newBooleanQuery() {
		BooleanQuery.Builder builder = new BooleanQuery.Builder();
		builder.setMinimumNumberShouldMatch(minimumNumberShouldMatch);
		return builder;
	}

	@Override
	protected Query getWildcardQuery(String field, String termStr) throws ParseException {
		if (termStr.equals("*") && !field.equals("*")) {
			return new TermQuery(new Term(ZuliaConstants.FIELDS_LIST_FIELD, field));
		}
		return super.getWildcardQuery(field, termStr);
	}
}
