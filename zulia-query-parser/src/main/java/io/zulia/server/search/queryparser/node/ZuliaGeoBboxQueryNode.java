package io.zulia.server.search.queryparser.node;

import io.zulia.server.field.FieldTypeUtil;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.queryparser.flexible.core.parser.EscapeQuerySyntax;
import org.apache.lucene.queryparser.flexible.standard.parser.EscapeQuerySyntaxImpl;
import org.apache.lucene.search.Query;

import java.util.Locale;
import java.util.Objects;

public class ZuliaGeoBboxQueryNode extends ZuliaFieldableQueryNode {
	private final CharSequence geoField;
	private final double minLat;
	private final double maxLat;
	private final double minLon;
	private final double maxLon;

	public ZuliaGeoBboxQueryNode(CharSequence geoField, double minLat, double maxLat, double minLon, double maxLon) {
		setField(geoField);
		this.geoField = geoField;
		this.minLat = minLat;
		this.maxLat = maxLat;
		this.minLon = minLon;
		this.maxLon = maxLon;
	}

	@Override
	public Query getQuery() {
		Objects.requireNonNull(getIndexFieldInfo(), "Field " + geoField + " must be indexed for geo bbox queries");
		if (!FieldTypeUtil.isGeoPointFieldType(getIndexFieldInfo().getFieldType())) {
			throw new IllegalArgumentException("Field " + geoField + " is not a GEO_POINT field");
		}
		String internalFieldName = getIndexFieldInfo().getInternalFieldName();
		return LatLonPoint.newBoxQuery(internalFieldName, minLat, maxLat, minLon, maxLon);
	}

	@Override
	public String toQueryString(EscapeQuerySyntax escapeSyntaxParser) {
		return String.format(Locale.ROOT, "%s:geoBbox(%s %s %s %s)", geoField, minLat, maxLat, minLon, maxLon);
	}

	@Override
	public String toString() {
		return toQueryString(new EscapeQuerySyntaxImpl());
	}

	@Override
	public ZuliaGeoBboxQueryNode cloneTree() {
		return new ZuliaGeoBboxQueryNode(geoField, minLat, maxLat, minLon, maxLon);
	}
}
