package com.linklogis.override;

import com.amazonaws.services.s3.model.S3ObjectId;

/**
 * <p>
 * 此类继承自 {@link com.amazonaws.services.s3.model.GetObjectRequest}
 * </p>
 * <p>
 * 只是为了重写 {@link GetObjectRequest#toString()}方法
 * </p>
 * <p>
 * 具体使用文档请参阅父类
 * </p>
 */
public class GetObjectRequest extends com.amazonaws.services.s3.model.GetObjectRequest {

    /**
     * <p>
     * {@link com.amazonaws.services.s3.model.GetObjectRequest#GetObjectRequest(String, String)}
     * </p>
     */
    public GetObjectRequest(String bucketName, String key) {
        super(bucketName, key);
    }

    /**
     * <p>
     * {@link com.amazonaws.services.s3.model.GetObjectRequest#GetObjectRequest(String, String, String)}
     * </p>
     */
    public GetObjectRequest(String bucketName, String key, String versionId) {
        super(bucketName, key, versionId);
    }

    /**
     * <p>
     * {@link com.amazonaws.services.s3.model.GetObjectRequest#GetObjectRequest(S3ObjectId)}
     * </p>
     */
    public GetObjectRequest(S3ObjectId s3ObjectId) {
        super(s3ObjectId);
    }

    /**
     * <p>
     * {@link com.amazonaws.services.s3.model.GetObjectRequest#GetObjectRequest(String, String, boolean)}
     * </p>
     */
    public GetObjectRequest(String bucketName, String key, boolean isRequesterPays) {
        super(bucketName, key, isRequesterPays);
    }

    @Override
    public String toString() {
        return " * Bucket Name      : " + getBucketName() + "\n"
                + " * Key              : " + getKey() + "\n"
                + " * VersionId        : " + getVersionId() + "\n"
                + " * S3ObjectId       : " + getS3ObjectId() + "\n"
                + " * Is Requester Pays: " + isRequesterPays();
    }
}
