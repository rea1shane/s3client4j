package com.linklogis;

/**
 * 此类继承自 {@link com.amazonaws.services.s3.model.ListObjectsRequest}，只是为了重写 {@link ListObjectsRequest#toString()}方法
 * 具体使用文档请参阅父类
 */
public class ListObjectsRequest extends com.amazonaws.services.s3.model.ListObjectsRequest {
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
