package com.linklogis.override;

/**
 * <p>
 * 此类继承自 {@link com.amazonaws.services.s3.model.CopyObjectRequest}
 * </p>
 * <p>
 * 只是为了重写 {@link CopyObjectRequest#toString()}方法
 * </p>
 * <p>
 * 具体使用文档请参阅父类
 * </p>
 */
public class CopyObjectRequest extends com.amazonaws.services.s3.model.CopyObjectRequest {

    /**
     * <p>
     * {@link com.amazonaws.services.s3.model.CopyObjectRequest#CopyObjectRequest()}
     * </p>
     */
    public CopyObjectRequest() {
    }

    /**
     * <p>
     * {@link com.amazonaws.services.s3.model.CopyObjectRequest#CopyObjectRequest(String, String, String, String)}
     * </p>
     */
    public CopyObjectRequest(String sourceBucketName, String sourceKey, String destinationBucketName, String destinationKey) {
        super(sourceBucketName, sourceKey, destinationBucketName, destinationKey);
    }

    /**
     * <p>
     * {@link com.amazonaws.services.s3.model.CopyObjectRequest#CopyObjectRequest(String, String, String, String, String)}
     * </p>
     */
    public CopyObjectRequest(String sourceBucketName, String sourceKey, String sourceVersionId, String destinationBucketName, String destinationKey) {
        super(sourceBucketName, sourceKey, sourceVersionId, destinationBucketName, destinationKey);
    }

    @Override
    public String toString() {
        return " * Source Bucket Name     : " + getSourceBucketName() + "\n"
                + " * Source Key             : " + getSourceKey() + "\n"
                + " * Source VersionId       : " + getSourceVersionId() + "\n"
                + " * Destination Bucket Name: " + getDestinationBucketName() + "\n"
                + " * Destination Key        : " + getDestinationKey();
    }
}
