package com.linklogis.override;

import com.amazonaws.services.s3.model.ObjectMetadata;

import java.io.File;
import java.io.InputStream;

/**
 * <p>
 * 此类继承自 {@link com.amazonaws.services.s3.model.PutObjectRequest}
 * </p>
 * <p>
 * 只是为了重写 {@link PutObjectRequest#toString()}方法
 * </p>
 * <p>
 * 具体使用文档请参阅父类
 * </p>
 */
public class PutObjectRequest extends com.amazonaws.services.s3.model.PutObjectRequest {

    /**
     * <p>
     * {@link com.amazonaws.services.s3.model.PutObjectRequest#PutObjectRequest(String, String, File)}
     * </p>
     */
    public PutObjectRequest(String bucketName, String key, File file) {
        super(bucketName, key, file);
    }

    /**
     * <p>
     * {@link com.amazonaws.services.s3.model.PutObjectRequest#PutObjectRequest(String, String, String)}
     * </p>
     */
    public PutObjectRequest(String bucketName, String key, String redirectLocation) {
        super(bucketName, key, redirectLocation);
    }

    /**
     * <p>
     * {@link com.amazonaws.services.s3.model.PutObjectRequest#PutObjectRequest(String, String, InputStream, ObjectMetadata)}
     * </p>
     */
    public PutObjectRequest(String bucketName, String key, InputStream input, ObjectMetadata metadata) {
        super(bucketName, key, input, metadata);
    }

    @Override
    public String toString() {
        return " * Bucket Name      : " + getBucketName() + "\n"
                + " * Key              : " + getKey() + "\n"
                + " * File             : " + getFile() + "\n"
                + " * Redirect Location: " + getRedirectLocation() + "\n"
                + " * Input Stream     : " + getInputStream() + "\n"
                + " * Metadata         : " + getMetadata();
    }
}
