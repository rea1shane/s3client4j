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
     * {@link com.amazonaws.services.s3.model.GetObjectRequest#GetObjectRequest(String, String)}
     */
    public GetObjectRequest(String bucketName, String key) {
        super(bucketName, key);
    }

    /**
     * {@link com.amazonaws.services.s3.model.GetObjectRequest#GetObjectRequest(String, String, String)}
     */
    public GetObjectRequest(String bucketName, String key, String versionId) {
        super(bucketName, key, versionId);
    }

    /**
     * {@link com.amazonaws.services.s3.model.GetObjectRequest#GetObjectRequest(S3ObjectId)}
     */
    public GetObjectRequest(S3ObjectId s3ObjectId) {
        super(s3ObjectId);
    }

    /**
     * {@link com.amazonaws.services.s3.model.GetObjectRequest#GetObjectRequest(String, String, boolean)}
     */
    public GetObjectRequest(String bucketName, String key, boolean isRequesterPays) {
        super(bucketName, key, isRequesterPays);
    }

    @Override
    public String toString() {
        return "* BucketName     : " + getBucketName() + "\n"
                + "* Key            : " + getKey() + "\n"
                + "* VersionId      : " + getVersionId() + "\n"
                + "* S3ObjectId     : " + getS3ObjectId() + "\n"
                + "* IsRequesterPays: " + isRequesterPays();
    }
}
