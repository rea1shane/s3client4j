package com.linklogis.override;

/**
 * 此类继承自 {@link com.amazonaws.services.s3.model.ListObjectsRequest}
 * 只是为了重写 {@link ListObjectsRequest#toString()}方法
 * 具体使用文档请参阅父类
 */
public class ListObjectsRequest extends com.amazonaws.services.s3.model.ListObjectsRequest {

    /**
     * {@link com.amazonaws.services.s3.model.ListObjectsRequest#ListObjectsRequest()}
     */
    public ListObjectsRequest() {
    }

    /**
     * {@link com.amazonaws.services.s3.model.ListObjectsRequest#ListObjectsRequest(String, String, String, String, Integer)}
     */
    public ListObjectsRequest(String bucketName, String prefix, String marker, String delimiter, Integer maxKeys) {
        super(bucketName, prefix, marker, delimiter, maxKeys);
    }

    @Override
    public String toString() {
        return "* BucketName         : " + getBucketName() + "\n"
                + "* Prefix             : " + getPrefix() + "\n"
                + "* Marker             : " + getMarker() + "\n"
                + "* Delimiter          : " + getDelimiter() + "\n"
                + "* MaxKeys            : " + getMaxKeys() + "\n"
                + "* EncodingType       : " + getEncodingType() + "\n"
                + "* IsRequesterPays    : " + isRequesterPays() + "\n"
                + "* ExpectedBucketOwner: " + getExpectedBucketOwner();
    }
}
