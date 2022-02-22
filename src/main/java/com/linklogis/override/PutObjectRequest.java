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
     * {@link com.amazonaws.services.s3.model.PutObjectRequest#PutObjectRequest(String, String, File)}
     */
    public PutObjectRequest(String bucketName, String key, File file) {
        super(bucketName, key, file);
    }

    /**
     * {@link com.amazonaws.services.s3.model.PutObjectRequest#PutObjectRequest(String, String, String)}
     */
    public PutObjectRequest(String bucketName, String key, String redirectLocation) {
        super(bucketName, key, redirectLocation);
    }

    /**
     * {@link com.amazonaws.services.s3.model.PutObjectRequest#PutObjectRequest(String, String, InputStream, ObjectMetadata)}
     */
    public PutObjectRequest(String bucketName, String key, InputStream input, ObjectMetadata metadata) {
        super(bucketName, key, input, metadata);
    }

    @Override
    public String toString() {
        return "* BucketName      : " + getBucketName() + "\n"
                + "* Key             : " + getKey() + "\n"
                + "* File            : " + getFile() + "\n"
                + "* RedirectLocation: " + getRedirectLocation() + "\n"
                + "* InputStream     : " + getInputStream() + "\n"
                + "* Metadata        : " + getMetadata();
    }
}
