package io.zulia.server.index;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.SloppyMath;

import java.io.IOException;
import java.util.Objects;

/**
 * A {@link DoubleValuesSource} that computes haversine distance in meters from a
 * {@link org.apache.lucene.document.LatLonDocValuesField} to a given origin point.
 * Returns meters to match Lucene's convention (e.g., {@link org.apache.lucene.document.LatLonDocValuesField#newDistanceSort}).
 */
public class GeoDistanceValuesSource extends DoubleValuesSource {

	private final String field;
	private final double originLat;
	private final double originLon;

	public GeoDistanceValuesSource(String field, double originLat, double originLon) {
		this.field = field;
		this.originLat = originLat;
		this.originLon = originLon;
	}

	@Override
	public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
		SortedNumericDocValues sortedNumeric = DocValues.getSortedNumeric(ctx.reader(), field);
		return new DoubleValues() {
			@Override
			public double doubleValue() throws IOException {
				long encoded = sortedNumeric.nextValue();
				double lat = GeoEncodingUtils.decodeLatitude((int) (encoded >> 32));
				double lon = GeoEncodingUtils.decodeLongitude((int) (encoded & 0xFFFFFFFFL));
				return SloppyMath.haversinMeters(originLat, originLon, lat, lon);
			}

			@Override
			public boolean advanceExact(int target) throws IOException {
				return sortedNumeric.advanceExact(target);
			}
		};
	}

	@Override
	public boolean needsScores() {
		return false;
	}

	@Override
	public DoubleValuesSource rewrite(IndexSearcher reader) {
		return this;
	}

	@Override
	public int hashCode() {
		return Objects.hash(field, originLat, originLon);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null || getClass() != obj.getClass()) {
			return false;
		}
		GeoDistanceValuesSource other = (GeoDistanceValuesSource) obj;
		return Objects.equals(field, other.field) && Double.compare(originLat, other.originLat) == 0
				&& Double.compare(originLon, other.originLon) == 0;
	}

	@Override
	public String toString() {
		return "geodist(" + field + ", " + originLat + ", " + originLon + ")";
	}

	@Override
	public boolean isCacheable(LeafReaderContext ctx) {
		return DocValues.isCacheable(ctx, field);
	}
}
