package io.zulia.util;

/**
 * A geo point in degrees. Latitude must lie within [-90, 90] and longitude within [-180, 180], the same bounds Lucene
 * enforces when the point is indexed, so a bad coordinate is caught with a clear message before it reaches Lucene.
 */
public record LatLon(double latitude, double longitude) {

	public LatLon {
		if (!isValidLatitude(latitude)) {
			throw new IllegalArgumentException(invalidLatitude(latitude));
		}
		if (!isValidLongitude(longitude)) {
			throw new IllegalArgumentException(invalidLongitude(longitude));
		}
	}

	public static boolean isValidLatitude(double latitude) {
		return latitude >= -90.0 && latitude <= 90.0;
	}

	public static boolean isValidLongitude(double longitude) {
		return longitude >= -180.0 && longitude <= 180.0;
	}

	public static String invalidLatitude(double latitude) {
		return "latitude <" + latitude + "> must be between -90 and 90";
	}

	public static String invalidLongitude(double longitude) {
		return "longitude <" + longitude + "> must be between -180 and 180";
	}
}
