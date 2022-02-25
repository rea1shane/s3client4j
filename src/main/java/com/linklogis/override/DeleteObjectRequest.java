package com.linklogis.override;

/**
 * <p>
 * 此类继承自 {@link com.amazonaws.services.s3.model.DeleteObjectRequest}
 * </p>
 * <p>
 * 只是为了重写 {@link DeleteObjectRequest#toString()}方法
 * </p>
 * <p>
 * 具体使用文档请参阅父类
 * </p>
 */
public class DeleteObjectRequest extends com.amazonaws.services.s3.model.DeleteObjectRequest {

    /**
     * <p>
     * {@link com.amazonaws.services.s3.model.DeleteObjectRequest#DeleteObjectRequest(String, String)}
     * </p>
     */
    public DeleteObjectRequest(String bucketName, String key) {
        super(bucketName, key);
    }

    @Override
    public String toString() {
        return " * Bucket Name: " + getBucketName() + "\n"
                + " * Key        : " + getKey();
    }
}
