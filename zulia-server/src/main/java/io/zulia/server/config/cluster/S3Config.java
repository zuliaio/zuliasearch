package io.zulia.server.config.cluster;

public class S3Config {
    private String s3BucketName;
    private String region;
    private boolean propWait = false;

    public String getS3BucketName() {
        return s3BucketName;
    }

    public void setS3BucketName(String s3BucketName) {
        this.s3BucketName = s3BucketName;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public boolean isPropWait() {
        return propWait;
    }

    public void setPropWait(boolean propWait) {
        this.propWait = propWait;
    }

    public S3Config() {

    }

    @Override
    public String toString() {
        return "S3Config{" + "s3BucketName='" + s3BucketName + '\'' + ", region='" + region + '\'' + ", propWait=" + propWait + '}';
    }
}
