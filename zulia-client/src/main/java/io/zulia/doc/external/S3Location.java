package io.zulia.doc.external;

import org.bson.Document;

public class S3Location extends Document {
	private static final String BUCKET = "bucket";
	private static final String REGION = "region";
	private static final String KEY = "key";

	public static Builder newBuilder() {
		return new Builder();
	}

	public static final class Builder {
		private String bucket;
		private String region;
		private String key;

		public Builder withBucket(String bucket) {
			this.bucket = bucket;
			return this;
		}

		public Builder withRegion(String region) {
			this.region = region;
			return this;
		}

		public Builder withKey(String key) {
			this.key = key;
			return this;
		}

		public S3Location build() {
			assert (null != key && null != bucket && null != region);
			return new S3Location(bucket, region, key);
		}
	}

	private S3Location(String bucket, String region, String key) {
		this.put(BUCKET, bucket);
		this.put(REGION, region);
		this.put(KEY, key);
	}

	public String getBucket() {
		return this.getString(BUCKET);
	}

	public String getRegion() {
		return this.getString(REGION);
	}

	public String getKey() {
		return this.getString(KEY);
	}
}
