package io.zulia.server.search;

import io.zulia.ZuliaConstants;
import io.zulia.message.ZuliaIndex.FieldConfig;
import io.zulia.server.config.ServerIndexConfig;
import io.zulia.server.field.FieldTypeUtil;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
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

		field = rewriteLengthFields(field);

		if (field.startsWith(ZuliaConstants.CHAR_LENGTH_PREFIX) || field.startsWith(ZuliaConstants.LIST_LENGTH_PREFIX)) {
			return getNumericOrDateRange(field, start, end, startInclusive, endInclusive, FieldConfig.FieldType.NUMERIC_INT);
		}

		FieldConfig.FieldType fieldType = indexConfig.getFieldTypeForIndexField(field);
		if (FieldTypeUtil.isNumericOrDateFieldType(fieldType)) {
			return getNumericOrDateRange(field, start, end, startInclusive, endInclusive);
		}

		return super.getRangeQuery(field, start, end, startInclusive, endInclusive);

	}

	public static String rewriteLengthFields(String field) {
		if (field.startsWith("|||") && field.endsWith("|||")) {
			field = ZuliaConstants.LIST_LENGTH_PREFIX + field.substring(3, field.length() - 3);
		}
		else if (field.startsWith("||") && field.endsWith("||")) {

		}
		else if (field.startsWith("|") && field.endsWith("|")) {
			field = ZuliaConstants.CHAR_LENGTH_PREFIX + field.substring(1, field.length() - 1);
		}
		return field;
	}

	private Query getNumericOrDateRange(final String fieldName, final String start, final String end, final boolean startInclusive,
			final boolean endInclusive) {
		FieldConfig.FieldType fieldType = indexConfig.getFieldTypeForIndexField(fieldName);
		return getNumericOrDateRange(fieldName, start, end, startInclusive, endInclusive, fieldType);
	}

	private Query getNumericOrDateRange(String fieldName, String start, String end, boolean startInclusive, boolean endInclusive,
			FieldConfig.FieldType fieldType) {
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
	protected Query newTermQuery(Term term, float boost) {
		String field = term.field();
		String text = term.text();

		field = rewriteLengthFields(field);

		if (field.startsWith(ZuliaConstants.CHAR_LENGTH_PREFIX) || field.startsWith(ZuliaConstants.LIST_LENGTH_PREFIX)) {
			return new BoostQuery(IntPoint.newExactQuery(field, Integer.parseInt(text)), boost);
		}

		FieldConfig.FieldType fieldType = indexConfig.getFieldTypeForIndexField(field);
		if (FieldTypeUtil.isNumericOrDateFieldType(fieldType)) {
			if (FieldTypeUtil.isDateFieldType(fieldType)) {
				try {
					Long dateLong = getDateAsLong(text);
					return new BoostQuery(LongPoint.newExactQuery(field, dateLong), boost);
				}
				catch (Exception e) {
					return new MatchNoDocsQuery(field + " expects date");
				}
			}
			else {
				try {
					if (FieldTypeUtil.isNumericIntFieldType(fieldType)) {
						return new BoostQuery(IntPoint.newExactQuery(field, Integer.parseInt(text)), boost);
					}
					else if (FieldTypeUtil.isNumericLongFieldType(fieldType)) {
						return new BoostQuery(LongPoint.newExactQuery(field, Long.parseLong(text)), boost);
					}
					else if (FieldTypeUtil.isNumericFloatFieldType(fieldType)) {
						return new BoostQuery(FloatPoint.newExactQuery(field, Float.parseFloat(text)), boost);
					}
					else if (FieldTypeUtil.isNumericDoubleFieldType(fieldType)) {
						return new BoostQuery(DoublePoint.newExactQuery(field, Double.parseDouble(text)), boost);
					}
				}
				catch (NumberFormatException e) {
					return new MatchNoDocsQuery(field + " expects numeric of type <" + fieldType + ">");
				}
			}

		}

		return super.newTermQuery(term, boost);
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
