package io.zulia.server.search.queryparser.node;

import io.zulia.server.field.FieldTypeUtil;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.queryparser.flexible.core.parser.EscapeQuerySyntax;
import org.apache.lucene.queryparser.flexible.standard.parser.EscapeQuerySyntaxImpl;
import org.apache.lucene.search.Query;

import java.util.Locale;
import java.util.Objects;

public class ZuliaGeoDistanceQueryNode extends ZuliaFieldableQueryNode {
	private final CharSequence geoField;
	private final double latitude;
	private final double longitude;
	private final double distanceKm;

	public ZuliaGeoDistanceQueryNode(CharSequence geoField, double latitude, double longitude, double distanceKm) {
		setField(geoField);
		this.geoField = geoField;
		this.latitude = latitude;
		this.longitude = longitude;
		this.distanceKm = distanceKm;
	}

	@Override
	public Query getQuery() {
		Objects.requireNonNull(getIndexFieldInfo(), "Field " + geoField + " must be indexed for geo queries");
		if (!FieldTypeUtil.isGeoPointFieldType(getIndexFieldInfo().getFieldType())) {
			throw new IllegalArgumentException("Field " + geoField + " is not a GEO_POINT field");
		}
		String internalFieldName = getIndexFieldInfo().getInternalFieldName();
		double distanceMeters = distanceKm * 1000.0;
		return LatLonPoint.newDistanceQuery(internalFieldName, latitude, longitude, distanceMeters);
	}

	@Override
	public String toQueryString(EscapeQuerySyntax escapeSyntaxParser) {
		return String.format(Locale.ROOT, "%s:geo(%s %s %s)", geoField, latitude, longitude, distanceKm);
	}

	@Override
	public String toString() {
		return toQueryString(new EscapeQuerySyntaxImpl());
	}

	@Override
	public ZuliaGeoDistanceQueryNode cloneTree() {
		return new ZuliaGeoDistanceQueryNode(geoField, latitude, longitude, distanceKm);
	}
}
