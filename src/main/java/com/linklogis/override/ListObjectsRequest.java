package com.linklogis.override;

/**
 * <p>
 * 此类继承自 {@link com.amazonaws.services.s3.model.ListObjectsRequest}
 * </p>
 * <p>
 * 只是为了重写 {@link ListObjectsRequest#toString()}方法
 * </p>
 * <p>
 * 具体使用文档请参阅父类
 * </p>
 */
public class ListObjectsRequest extends com.amazonaws.services.s3.model.ListObjectsRequest {

    /**
     * <p>
     * {@link com.amazonaws.services.s3.model.ListObjectsRequest#ListObjectsRequest()}
     * </p>
     */
    public ListObjectsRequest() {
    }

    /**
     * <p>
     * {@link com.amazonaws.services.s3.model.ListObjectsRequest#ListObjectsRequest(String, String, String, String, Integer)}
     * </p>
     */
    public ListObjectsRequest(String bucketName, String prefix, String marker, String delimiter, Integer maxKeys) {
        super(bucketName, prefix, marker, delimiter, maxKeys);
    }

    @Override
    public String toString() {
        return " * Bucket Name: " + getBucketName() + "\n"
                + " * Prefix     : " + getPrefix() + "\n"
                + " * Marker     : " + getMarker() + "\n"
                + " * Delimiter  : " + getDelimiter() + "\n"
                + " * Max Keys   : " + getMaxKeys();
    }
}
